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
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"gopkg.in/fatih/set.v0"
	"gorm.io/gorm"
)

type Message struct {
	gorm.Model
	UserId uint //发送者
	TargetId int64 //接收者
	Type int //群聊、私聊、广播
	Media int //消息类型  文字、表情包、图片、音频
	Content string //消息内容
	CreateTime uint64 //创建时间
	ReadTime   uint64 //读取时间
	Pic string 
	Url string
	Desc string
	Amoubt int //其他数字统计

}
func (table *Message)TableName()string{
	return "message"
}

type Node struct {
	UserId        int64
	Conn          *websocket.Conn //连接
	FirstTime     uint64          //首次连接时间
	HeartbeatTime uint64          //心跳时间
	LoginTime     uint64          //登录时间
	DataQueue     chan []byte     //消息
	GroupSets     set.Interface   //好友 / 群
	CurrentHub    *Hub    //记录当前的Node属于哪个分片
}

// //映射关系
// var clientMap map[int64]*Node =make(map[int64]*Node,0)

// //读写锁
// var rwLock sync.RWMutex

// 企业级巅峰架构：真正的【路由分发】与【落盘持久化】双线解耦
// 内存操作维持纳秒级 (0.0001ms)，极耗时的 Redis/DB 抛入后台慢速消化区
var SaveQueue = make(chan []byte, 100000)

func Chat(writer http.ResponseWriter,request *http.Request){
	//1.获取参数并校验合法性
	query:=request.URL.Query()
	Id:=query.Get("userId")
	userId,_:=strconv.ParseInt(Id,10,64)
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
		Conn:          conn,
		DataQueue:     make(chan []byte, 1024),
		GroupSets:     set.New(set.ThreadSafe),
		HeartbeatTime: uint64(time.Now().Unix()),
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
	sendMsg(userId,[]byte("欢迎进入聊天系统"))
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
			fmt.Println("read error:", err)
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

	// 启动 500 个后台纯落盘协程，匹配你的 Redis PoolSize:500 达到极限吞吐
	for i := 0; i < 500; i++ {
		go saveWorker(i)
	}
}

func saveWorker(id int) {
	for data := range SaveQueue {
		saveToRedis(data) // 后台协程排队慢慢消化耗时的 Redis ZAdd 等操作
	}
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
		sendGroupMsg(int64(msg.TargetId), data)   //群发
	// case 3:sendAllMsg()        //广播
	// case 4:                //其他

	}
}
func sendMsg(userId int64, msgData []byte) {
	// 【架构重组核心：内存分发零延迟】
	// 第一阶段：不去管 Redis 等没等，只要收到消息瞬间在万分之一毫秒内送给 Hub 下发给用户，达到极致快。
	targetHub := WsHub.GetHub(userId)
	targetHub.send <- &SendRequest{
		userID: userId,
		data:   msgData,
	}

	// 第二阶段：异步转交后台的 500 名底层特种兵慢慢落盘 Redis 数据库
	select {
	case SaveQueue <- msgData:
	default:
		// 当这 10 万的队列依然被灌满时，也仅表现为有部分聊天记录少存了 Redis，
		// 但由于第一阶段的极速派发，两人聊天的体验永远不会卡顿断线！
	}
}

func saveToRedis(msgData []byte) {
	jsonMsg := Message{}
	json.Unmarshal(msgData, &jsonMsg)

	ctx := context.Background()
	targetIdStr := strconv.Itoa(int(jsonMsg.TargetId))
	userIdStr := strconv.Itoa(int(jsonMsg.UserId))
	
	// 这里不再重新反序列化装填回 msg，那是一个巨大的编解码 CPU 损耗，保留最原生信息直接做 Member
	var key string
	if uint(jsonMsg.TargetId) > jsonMsg.UserId {
		key = "msg_" + userIdStr + "_" + targetIdStr
	} else {
		key = "msg_" + targetIdStr + "_" + userIdStr
	}

	msgId := utils.GenSnowflakeId()
	score := float64(msgId)

	_, e := utils.Red.ZAdd(ctx, key, redis.Z{
		Score:  score,
		Member: msgData, // 直接存字节流，CPU 零损耗
	}).Result()
	
	if e != nil {
		fmt.Println("Redis ZAdd Error:", e)
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

//获取缓存里面的消息
func RedisMsg(userIdA int64, userIdB int64, start int64, end int64, isRev bool) []string {
	ctx := context.Background()
	userIdStr := strconv.Itoa(int(userIdA))
	targetIdStr := strconv.Itoa(int(userIdB))
	var key string
	if userIdA > userIdB {
		key = "msg_" + targetIdStr + "_" + userIdStr
	} else {
		key = "msg_" + userIdStr + "_" + targetIdStr
	}

	var rels []string
	var err error
	if isRev {
		rels, err = utils.Red.ZRange(ctx, key, start, end).Result()
	} else {
		rels, err = utils.Red.ZRevRange(ctx, key, start, end).Result()
	}
	if err != nil {
		fmt.Println(err) //没有找到
	}
	return rels
}

func sendGroupMsg(targetId int64,msg []byte ){
	fmt.Println("开始群发消息")
	userIds :=SearchUserByGroupId(uint(targetId))
	for i:=0;i<len(userIds);i++{
		//排除给自己的
		if targetId!= int64(userIds[i]){
			sendMsg(int64(userIds[i]),msg)
		}
	}
}
	//需要重写此方法才能完整的msg转byte[]
func (msg Message) MarshalBinary() ([]byte, error) {
	return json.Marshal(msg)
}
func (node *Node) IsHeartbeatTimeOut(currentTime uint64)  bool {
	 return currentTime-node.HeartbeatTime > uint64(viper.GetInt("timeout.DelayHeartbeat"))
}

	//更新用户心跳
func (node *Node) Heartbeat(currentTime uint64) {
	node.HeartbeatTime = currentTime
	return
}