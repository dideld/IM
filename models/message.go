package models

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
	"webapp/utils"

	"github.com/gorilla/websocket"
	"github.com/spf13/viper"
	"gopkg.in/fatih/set.v0"
	"gorm.io/gorm"
)

type Message struct {
	gorm.Model   // 冷存落库时 ID 主键显式写入雪花ID（53bit，兼容 JS 安全整数与 Redis float64 score）
	UserId uint //发送者
	TargetId int64 //接收者
	Type int //1私聊、2群聊、3心跳；4未读事件、5已读回执、6发送确认(ACK)——4/5/6 仅服务端→客户端
	Media int //消息类型  文字、表情包、图片、音频
	Content string //消息内容
	CreateTime uint64 //创建时间
	ReadTime   uint64 //读取时间
	Pic string
	Url string
	Desc string
	Amount int //其他数字统计（语音时长等，前端 JSON 字段名即 amount）
	ReadCount int //群聊逐条已读计数：MarkRead 按已读窗口 (old,seq] 给他人消息 +1，水位单调天然防重计
	ClientMsgId string `gorm:"size:64"` //客户端生成的幂等键：重发/重放经 cm HASH 门禁去重，命中回补原 ACK（persistMessage）
	Seq     int64  `gorm:"uniqueIndex:uk_conv_seq"` //会话内严格递增序号：排序/游标/未读用（与雪花主键 msgId 双轨）
	ConvKey string `gorm:"uniqueIndex:uk_conv_seq;index"` //会话id（p_{小}_{大} / g_{群id}），唯一来源 convIdOf；与 Seq 组成复合唯一索引

}
func (table *Message)TableName()string{
	return "message"
}

type Node struct {
	UserId        int64
	DeviceId      string          //设备标识（前端 sessionStorage 生成）：同用户多端在线，同设备重连替换旧节点
	Token         string          //登录会话 token（心跳时给 Redis 会话滑动续期）
	Conn          *websocket.Conn //连接
	FirstTime     uint64          //首次连接时间
	LoginTime     uint64          //登录时间
	DataQueue     chan []byte     //消息
	GroupSets     set.Interface   //好友 / 群
	CurrentHub    *Hub    //记录当前的Node属于哪个分片
}

// 消息全链路（先落盘、后投递——store-and-forward）：
// WS 侧只做校验 + 同步投 Kafka（失败进应用层重投队列，超限才回 -1 ACK）；
// 投递/未读/ACK 全部在"冷层写入成功"之后由 deliverLanded 放大（见 deliver.go / store.go）。

func Chat(writer http.ResponseWriter,request *http.Request){
	//1.获取参数并校验合法性
	query:=request.URL.Query()
	Id:=query.Get("userId")
	userId,_:=strconv.ParseInt(Id,10,64)
	//鉴权：握手升级前校验登录会话 token（未登录/伪造 userId 的连接直接拒绝）
	//auth.devSkipWSToken 为压测/本地调试专用的 dev-only 开关（默认 false），生产严禁开启
	token := query.Get("token")
	if !viper.GetBool("auth.devSkipWSToken") && !utils.ValidateToken(token, userId) {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	isvalida:=true
	conn,err:=(&websocket.Upgrader{
		//token校验
		CheckOrigin:func(r *http.Request)bool{
			return isvalida
		},
	}).Upgrade(writer,request,nil)
	if err!=nil{
		fmt.Println(err)
		return
	}
	//2.获取conn
	targetHub:=WsHub.GetHub(userId)
	//4.userid和node绑定并加锁
	node := &Node{
		UserId:        userId,
		DeviceId:      query.Get("deviceId"),
		Token:         token,
		Conn:          conn,
		DataQueue:     make(chan []byte, 1024),
		GroupSets:     set.New(set.ThreadSafe),
		CurrentHub:    targetHub, // 记录分片以便后续注销
	}
	targetHub.register <- node

	// 注册初始心跳任务到时间轮
	delay := time.Duration(viper.GetInt("timeout.DelayHeartbeat")) * time.Second
	if delay <= 0 {
		delay = 60 * time.Second
	}
	utils.TW.AddTask(delay, userId, func() {
		if node.Conn != nil {
			node.Conn.Close()
		}
		if node.CurrentHub != nil {
			node.CurrentHub.unregister <- node
		}
	})

	//5.完成发送逻辑
	go sendProc(node)
	//6.完成接受的逻辑
	go recvProc(node)
	//系统提示语只做内存分发，不进入持久化链路
	deliverLocal(userId,[]byte("欢迎进入聊天系统"))
}
func sendProc(node *Node) {
	for {
		select {
		case data, ok := <-node.DataQueue:
			if !ok {
				// 当 DataQueue 被 close 时，由于消息积压等原因被踢出
				// 必须主动断开底层的 TCP 连接，让客户端知道自己被踢啦！否则它会一直在那里傻等。
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
func recvProc(node *Node) {
	defer func() {
		// 节点断开时，无论是由于自行断开还是被时间轮踢出，彻底安全取消时间轮任务
		utils.TW.RemoveTask(node.UserId)

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
			//fmt.Println("read error:", err)
			break
		}

		msg := Message{}
		err = json.Unmarshal(data, &msg)
		if err != nil {
			fmt.Println(err)
			continue
		}

		// 心跳
		if msg.Type == 3 {
			// 在线即续期登录会话（token 与用户会话集合一起滑动过期）
			utils.TouchToken(node.Token, node.UserId)
			// 将心跳延迟任务 O(1) 刷新到时间轮对应的未来槽位
			delay := time.Duration(viper.GetInt("timeout.DelayHeartbeat")) * time.Second
			if delay <= 0 {
				delay = 60 * time.Second
			}
			utils.TW.AddTask(delay, node.UserId, func() {
				if node.Conn != nil {
					node.Conn.Close()
				}
				if node.CurrentHub != nil {
					node.CurrentHub.unregister <- node
				}
			})
			continue
		}

		// 结构校验：参数非法即时回 -1 ACK（不入库、不重投——"参数非法"与"存储故障"语义分开），不断连
		if verr := validateMsg(&msg); verr != nil {
			ackTo(node.UserId, msg.ClientMsgId, convIdForAck(&msg), 0, 0, -1, verr.Error())
			continue
		}

		// 唯一动作：校验通过后同步入持久化管线（投递/ACK 在落盘成功后由消费侧完成）
		dispatchTo(data, node.UserId)
	}
}

func init() {
	go udpRevProc()

}

// StartKafkaConsumer 对外暴露给 main 函数，用于注册消费群组回调（冷热双层持久化）
func StartKafkaConsumer() {
	utils.StartKafkaConsumer(persistMessage)
}

func udpRevProc(){
	con,err:=net.ListenUDP("udp",&net.UDPAddr{
		IP: net.IPv4zero,
		Port: viper.GetInt("port.udp"),

	})
	if err!=nil{
		fmt.Println(err)
	}
	defer con.Close()
	for {
		var buf [512]byte
		n, err := con.Read(buf[0:])
		if err != nil {
			fmt.Println(err)
			return
		}

		// 分配新内存防止在异步 Worker 处理被缓冲数组下一次 Read 覆盖
		msgData := make([]byte, n)
		copy(msgData, buf[0:n])

		// UDP 的情况也只是单纯派发路由
		dispatch(msgData)
	}
}

//后端调度逻辑处理（UDP 旁路：无 ACK 归属，失败仅日志）
func dispatch(data []byte){
	dispatchTo(data, 0)
}

// dispatchTo 业务消息进入持久化管线的唯一路径：只做同步入队（produceOrDefer，
// 失败进应用层重投队列）。投递/未读/ACK 全部移到"冷层写入成功"之后的 deliverLanded——
// 这是"先落盘再投递、端到端可靠"的顺序闸门，WS 侧不再直接投递任何消息。
func dispatchTo(data []byte, ackUid int64) {
	msg := Message{}
	if err := json.Unmarshal(data, &msg); err != nil {
		fmt.Println(err)
		return
	}
	switch msg.Type {
	case 1, 2:
		if ackUid <= 0 {
			ackUid = int64(msg.UserId) // UDP 等无连接归属的路径按帧内 UserId 回执（尽力而为）
		}
		produceOrDefer(ackUid, &msg, data)
	}
}

// validateMsg 结构校验（WS 入口）。约定：业务消息必须携带 clientMsgId——
// 它是幂等键，也是 ACK 关联的唯一凭据，缺失则服务端无法承诺"发后必有所谓"。
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

// convIdForAck 为校验失败的帧尽量推导会话 id；类型不可识别时返回空串。
func convIdForAck(m *Message) string {
	if m.Type != 1 && m.Type != 2 {
		return ""
	}
	return convIdOf(m.Type, int64(m.UserId), m.TargetId)
}

// deliverLocal 仅做内存分发（欢迎语等系统提示使用，不进持久化链路）
func deliverLocal(userId int64, data []byte) {
	targetHub := WsHub.GetHub(userId)
	targetHub.send <- &SendRequest{
		userID: userId,
		data:   data,
	}
}

//加入群聊
func JoinGroup(userId uint, comId string) (int, string) {
	contact := Contact{}
	contact.OwnerId = userId
	//contact.TargetId = comId
	contact.Type = 2
	community := Community{}

	utils.DB.Where("id= ? or name= ?", comId, comId).Find(&community)
	if community.Name == "" {
		return -1, "没有找到群"
	}
	utils.DB.Where("owner_id= ? and target_id= ? and type =2 ", userId, comId).Find(&contact)
	if !contact.CreatedAt.IsZero() {
		return -1, "已加过此群"
	} else {
		contact.TargetId = community.ID
		utils.DB.Create(&contact)
		CacheGroupMember(community.ID, userId) // g_members 缓存即时补员（退群/解散靠 TTL 最终一致）
		return 0, "加群成功"
	}
}

//获取热层（Redis）缓存里面的消息，convId 为会话键（convIdOf 规则，见 store.go）
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
		fmt.Println(err) //没有找到
	}
	return rels
}

//sendGroupMsg 已废弃：群扇出移至落盘成功后的 deliverLanded（models/deliver.go）——
//消费者按 g_members 缓存逐人投递 + 未读，杜绝"先投递后落库"，也不回显发送者本人（其状态走 Type=6 ACK）。
	//需要重写此方法才能完整的msg转byte[]
func (msg Message) MarshalBinary() ([]byte, error) {
	return json.Marshal(msg)
}
