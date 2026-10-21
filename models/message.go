package models

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
	"webapp/utils"

	"github.com/gorilla/websocket"
	"github.com/spf13/viper"
	"gopkg.in/fatih/set.v0"
	"gorm.io/gorm"
)

// Message 消息模型（冷存储 = MySQL message 表，主键为雪花 ID）。
type Message struct {
	gorm.Model
	UserId      uint   // 发送者
	TargetId    int64  // 接收者
	Type        int    // 1=私聊，2=群聊，3=心跳；4/5/6 仅服务端→客户端（未读事件/已读回执/ACK）
	Media       int    // 0=文字，1=表情，2=图片，3=音频
	Content     string
	CreateTime  uint64
	ReadTime    uint64
	Pic         string
	Url         string
	Desc        string
	Amount      int
	ReadCount   int    // 群聊逐条已读计数（markReadWindowTx 行锁串行防双计）
	ClientMsgId string `gorm:"size:64;uniqueIndex:uk_conv_cmid,priority:2"` // 客户端幂等键（DB 复合唯一兜底 Redis 丢失后重放）
	Seq         int64  `gorm:"uniqueIndex:uk_conv_seq,priority:2"`           // 会话内严格递增序号（聚合器 INCRBY 发号）
	ConvKey     string `gorm:"uniqueIndex:uk_conv_seq,priority:1;uniqueIndex:uk_conv_cmid,priority:1;index"` // 会话 ID（唯一来源 convIdOf）
}

func (table *Message) TableName() string {
	return "message"
}

// Node 单个在线设备连接（Hub 分片内按 uid → []*Node 维护）。
type Node struct {
	UserId    int64
	DeviceId  string          // 设备标识（前端 sessionStorage 生成），同设备重连替换旧节点
	Token     string          // 登录会话 token（心跳时给 Redis 滑动续期）
	Conn      *websocket.Conn // WebSocket 连接
	FirstTime uint64          // 首次连接时间
	LoginTime uint64          // 登录时间
	DataQueue chan []byte     // 消息下发队列
	GroupSets set.Interface   // 好友/群集合
	CurrentHub *Hub           // 所属 Hub 分片
	hbGen     atomic.Int64    // 心跳代数（每次续期 +1，超时任务比对防误踢）
}

// hbKey 时间轮心跳任务的键：按"用户#设备"隔离——多端共享一个键时，
// 任一设备的心跳会替其他设备续命/误踢最后心跳的那台（P0 修复）。
// 命名空间 hb: 前缀，与 double_delete_ 等键不冲突。
func hbKey(node *Node) string {
	return "hb:" + strconv.FormatInt(node.UserId, 10) + "#" + node.DeviceId
}

// heartbeatKick 安装/刷新本设备的心跳超时任务（O(1)），代数守卫丢弃已续期的陈旧踢线
func heartbeatKick(node *Node) {
	delay := time.Duration(viper.GetInt("timeout.DelayHeartbeat")) * time.Second
	if delay <= 0 {
		delay = 60 * time.Second
	}
	gen := node.hbGen.Add(1)
	utils.TW.AddTask(delay, hbKey(node), func() {
		if node.hbGen.Load() != gen {
			return // 期间已有心跳，本任务过期
		}
		if node.Conn != nil {
			node.Conn.Close()
		}
		if node.CurrentHub != nil {
			node.CurrentHub.unregister <- node
		}
	})
}

// maxDevicesPerUser 单用户设备数上限（hub.maxDevices，缺省 5）
func maxDevicesPerUser() int {
	if n := viper.GetInt("hub.maxDevices"); n > 0 {
		return n
	}
	return 5
}

// 消息全链路（先落盘、后投递——store-and-forward）：
// WS 侧只做校验 + 同步投 Kafka（失败进应用层重投队列，超限才回 -1 ACK）；
// 投递/未读/ACK 全部在"冷层写入成功"之后由 deliverLanded 放大（见 deliver.go / store.go）。

// Chat WebSocket 连接升级入口：握手前校验 token 和设备数，升级后启动收发协程。
func Chat(writer http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	Id := query.Get("userId")
	userId, _ := strconv.ParseInt(Id, 10, 64)

	// NOTE: auth.devSkipWSToken 为压测/本地调试专用开关，生产严禁开启
	token := query.Get("token")
	if !viper.GetBool("auth.devSkipWSToken") && !utils.ValidateToken(token, userId) {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}

	deviceId := query.Get("deviceId")
	if n := WsHub.DeviceCount(userId, deviceId); n >= maxDevicesPerUser() {
		http.Error(writer, "device limit exceeded", http.StatusForbidden)
		return
	}

	conn, err := (&websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}).Upgrade(writer, request, nil)
	if err != nil {
		utils.Error(context.Background(), "WebSocket upgrade failed", "error", err)
		return
	}

	targetHub := WsHub.GetHub(userId)
	node := &Node{
		UserId:    userId,
		DeviceId:  deviceId,
		Token:     token,
		Conn:      conn,
		DataQueue: make(chan []byte, 1024),
		GroupSets: set.New(set.ThreadSafe),
		CurrentHub: targetHub,
	}
	targetHub.register <- node
	heartbeatKick(node)

	go sendProc(node)
	go recvProc(node)
	deliverLocal(userId, []byte("欢迎进入聊天系统"))
}
// sendProc 消息下发协程：从 DataQueue 读取数据写入 WebSocket，队列关闭时断开连接。
func sendProc(node *Node) {
	for {
		select {
		case data, ok := <-node.DataQueue:
			if !ok {
				node.Conn.Close()
				return
			}
			err := node.Conn.WriteMessage(websocket.TextMessage, data)
			if err != nil {
				return
			}
		}
	}
}

// recvProc 消息接收协程：读取 WebSocket 消息，校验后入持久化管线。
func recvProc(node *Node) {
	defer func() {
		utils.TW.RemoveTask(hbKey(node))
		if node != nil && node.CurrentHub != nil {
			node.CurrentHub.unregister <- node
		}
		if node != nil && node.Conn != nil {
			node.Conn.Close()
		}
	}()

	for {
		_, data, err := node.Conn.ReadMessage()
		if err != nil {
			break
		}

		msg := Message{}
		err = json.Unmarshal(data, &msg)
		if err != nil {
			utils.Error(context.Background(), "recvProc json.Unmarshal failed", "error", err)
			continue
		}

		// 心跳：续期会话 + 刷新时间轮任务
		if msg.Type == 3 {
			if !viper.GetBool("auth.devSkipWSToken") && !utils.TouchToken(node.Token, node.UserId) {
				utils.Warn(context.Background(), "session revoked/expired, closing device node", "userId", node.UserId, "deviceId", node.DeviceId)
				break
			}
			heartbeatKick(node)
			continue
		}

		// 结构校验：参数非法即时回 -1 ACK，不入库不重投
		if verr := validateMsg(&msg); verr != nil {
			ackTo(node.UserId, msg.ClientMsgId, convIdForAck(&msg), 0, 0, -1, verr.Error())
			continue
		}

		dispatchTo(data, node.UserId)
	}
}

func init() {
	go udpRevProc()
}

// udpRevProc UDP 旁路接收协程（无 ACK 归属，失败仅日志）。
func udpRevProc() {
	con, err := net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: viper.GetInt("port.udp"),
	})
	if err != nil {
		utils.Error(context.Background(), "udpRevProc ListenUDP failed", "error", err)
	}
	defer con.Close()
	for {
		var buf [512]byte
		n, err := con.Read(buf[0:])
		if err != nil {
			utils.Error(context.Background(), "udpRevProc read failed", "error", err)
			return
		}
		msgData := make([]byte, n)
		copy(msgData, buf[0:n])
		dispatch(msgData)
	}
}

// dispatch UDP 消息路由入口。
func dispatch(data []byte) {
	dispatchTo(data, 0)
}

// dispatchTo 业务消息进入持久化管线的唯一入口：同步入队 produceOrDefer，投递/ACK 在落盘成功后由消费侧完成。
func dispatchTo(data []byte, ackUid int64) {
	msg := Message{}
	if err := json.Unmarshal(data, &msg); err != nil {
		utils.Error(context.Background(), "dispatchTo json.Unmarshal failed", "error", err)
		return
	}
	switch msg.Type {
	case 1, 2:
		if ackUid <= 0 {
			ackUid = int64(msg.UserId)
		}
		produceOrDefer(ackUid, &msg, data)
	}
}

// validateMsg 消息结构校验：业务消息必须携带 clientMsgId（幂等键 + ACK 凭据）。
func validateMsg(m *Message) error {
	if m.Type != 1 && m.Type != 2 {
		return fmt.Errorf("未知消息类型: %d", m.Type)
	}
	if m.TargetId <= 0 {
		return fmt.Errorf("TargetId 非法")
	}
	if m.Content == "" && m.Url == "" && m.Pic == "" {
		return fmt.Errorf("消息体为空")
	}
	if m.ClientMsgId == "" {
		return fmt.Errorf("缺少 clientMsgId(幂等与 ACK 关联必需)")
	}
	return nil
}

// convIdForAck 为校验失败的帧尽量推导会话 ID。
func convIdForAck(m *Message) string {
	if m.Type != 1 && m.Type != 2 {
		return ""
	}
	return convIdOf(m.Type, int64(m.UserId), m.TargetId)
}

// deliverLocal 仅做内存分发（欢迎语等系统提示使用，不进持久化链路）。
func deliverLocal(userId int64, data []byte) {
	targetHub := WsHub.GetHub(userId)
	targetHub.send <- &SendRequest{
		userID: userId,
		data:   data,
	}
}

// JoinGroup 加入群聊。
func JoinGroup(userId uint, comId string) (int, string) {
	contact := Contact{}
	contact.OwnerId = userId
	contact.Type = 2
	community := Community{}

	utils.DB.Where("id= ? or name= ?", comId, comId).Find(&community)
	if community.Name == "" {
		return -1, "没有找到群"
	}
	utils.DB.Where("owner_id= ? and target_id= ? and type =2 ", userId, comId).Find(&contact)
	if !contact.CreatedAt.IsZero() {
		return -1, "已加过此群"
	}
	contact.TargetId = community.ID
	utils.DB.Create(&contact)
	CacheGroupMember(community.ID, userId)
	return 0, "加群成功"
}

// RedisMsg 获取热层（Redis）缓存消息。
func RedisMsg(convId string, start int64, end int64, isRev bool) []string {
	ctx := context.Background()

	var rels []string
	var err error
	if isRev {
		rels, err = utils.Red.ZRevRange(ctx, convId, start, end).Result()
	} else {
		rels, err = utils.Red.ZRange(ctx, convId, start, end).Result()
	}
	if err != nil {
		utils.Error(ctx, "Redis_Msg query failed", "error", err)
	}
	return rels
}

// MarshalBinary 消息序列化为 JSON（WebSocket 下发使用）。
func (msg Message) MarshalBinary() ([]byte, error) {
	return json.Marshal(msg)
}
