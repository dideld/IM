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
	Type int //1私聊、2群聊、3心跳
	Media int //消息类型  文字、表情包、图片、音频
	Content string //消息内容
	CreateTime uint64 //创建时间
	ReadTime   uint64 //读取时间
	Pic string
	Url string
	Desc string
	Amount int //其他数字统计（语音时长等，前端 JSON 字段名即 amount）
	Seq     int64  `gorm:"uniqueIndex:uk_conv_seq"` //会话内严格递增序号：排序/游标/未读用（与雪花主键 msgId 双轨）
	ConvKey string `gorm:"uniqueIndex:uk_conv_seq;index"` //会话id（p_{小}_{大} / g_{群id}），唯一来源 convIdOf；与 Seq 组成复合唯一索引

}
func (table *Message)TableName()string{
	return "message"
}

type Node struct {
	UserId        int64
	Token         string          //登录会话 token（心跳时给 Redis 会话滑动续期）
	Conn          *websocket.Conn //连接
	FirstTime     uint64          //首次连接时间
	LoginTime     uint64          //登录时间
	DataQueue     chan []byte     //消息
	GroupSets     set.Interface   //好友 / 群
	CurrentHub    *Hub    //记录当前的Node属于哪个分片
}

// 消息全链路：内存极速分发（WebSocket 低延迟） + Kafka 异步解耦持久化（冷热存储，见 store.go）

func Chat(writer http.ResponseWriter,request *http.Request){
	//1.获取参数并校验合法性
	query:=request.URL.Query()
	Id:=query.Get("userId")
	userId,_:=strconv.ParseInt(Id,10,64)
	//鉴权：握手升级前校验登录会话 token（未登录/伪造 userId 的连接直接拒绝）
	token := query.Get("token")
	if !utils.ValidateToken(token, userId) {
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

		// 第一步：内存极速路由！不再通过 MsgQueue 阻挡 WebSocket 发送，而是瞬间派发
		dispatch(data)
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

//后端调度逻辑处理
func dispatch(data []byte){
	msg:=Message{}
	err:=json.Unmarshal(data,&msg)
	if err!=nil{
		fmt.Println(err)
		return
	}
	switch msg.Type{
	 case 1:
		//fmt.Println("dispatch  data :", string(data))
		sendMsg(int64(msg.TargetId),data)   //私信
	 case 2:
		//群消息持久化"一条一份"进 g_{群id}（此前每个收件人扇出重复投 Kafka，会在同一会话落 N 份拷贝）；
		//实时投递仍按成员逐个内存分发
		utils.SendMsgToKafka(data)
		sendGroupMsg(int64(msg.TargetId), data)   //群发
	// case 3:sendAllMsg()        //广播
	// case 4:                //其他

	}
}
func sendMsg(userId int64, msgData []byte) {
	// 【架构核心：两阶段投递】
	// 第一阶段：内存分发零延迟，瞬间送给 Hub 下发给用户。
	deliverLocal(userId, msgData)

	// 第二阶段：通过 Kafka 跨进程投递给消费者组，冷热双层落存储。
	// 哪怕 Go 服务崩溃重启，消息也不会丢！
	utils.SendMsgToKafka(msgData)
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

func sendGroupMsg(targetId int64,msg []byte ){
	fmt.Println("开始群发消息")
	//解析发送者：群消息不再回显给发送者本人
	//（旧代码误拿群 id 与成员 id 比较，导致自己收到自己发的消息并产生 msg_自己_自己 的持久化副本）
	m := Message{}
	if err := json.Unmarshal(msg, &m); err != nil {
		fmt.Println(err)
		return
	}
	userIds :=SearchUserByGroupId(uint(targetId))
	for i:=0;i<len(userIds);i++{
		//排除发送者自己；仅内存分发（持久化已在 dispatch 里统一做一次）
		if int64(m.UserId) != int64(userIds[i]){
			deliverLocal(int64(userIds[i]),msg)
		}
	}
}
	//需要重写此方法才能完整的msg转byte[]
func (msg Message) MarshalBinary() ([]byte, error) {
	return json.Marshal(msg)
}
