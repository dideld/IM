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
		DataQueue:     make(chan []byte, 256),
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
func sendProc(node *Node){
	for{
		select{
		case data:=<-node.DataQueue:
			//fmt.Println("[ws]sendProc >>> msg",string(data))
			err:=node.Conn.WriteMessage(websocket.TextMessage,data)
			if err!=nil{
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

		dispatch(data)
	}
}

func init(){
	go udpRevProc() 
	//go WsHub.Run()
	//fmt.Println("init goroutine:------------")
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
	for{
		var buf [512]byte
		n,err:=con.Read(buf[0:])
		if err!=nil{
			fmt.Println(err)
			return 
		}
		//fmt.Println("udpRevProc  data :", string(buf[0:n]))
		dispatch(buf[0:n])
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
func sendMsg(userId int64, msg []byte) {

	jsonMsg := Message{}
	json.Unmarshal(msg, &jsonMsg)

	ctx := context.Background()
	targetIdStr := strconv.Itoa(int(userId))
	userIdStr := strconv.Itoa(int(jsonMsg.UserId))
	//设置时间戳
	jsonMsg.CreateTime = uint64(time.Now().Unix())
	var key string
	if userId > int64(jsonMsg.UserId) {
		key = "msg_" + userIdStr + "_" + targetIdStr
	} else {
		key = "msg_" + targetIdStr + "_" + userIdStr
	}
	// --- 核心优化点：引入特制 53 Bits 雪花算法生成绝对唯一且递增的消息 ID ---
	// 替换掉之前性能极差且有大坑的 ZRevRange 取 cap 作为 score 的做法
	msgId := utils.GenSnowflakeId()
	score := float64(msgId)

	ress, e := utils.Red.ZAdd(ctx, key, redis.Z{
		Score:  score,
		Member: msg,
	}).Result() //jsonMsg
	//res, e := utils.Red.Do(ctx, "zadd", key, 1, jsonMsg).Result() //备用 后续拓展 记录完整msg
	if e != nil {
		fmt.Println(e)
	}
	fmt.Println(ress)
	targetHub :=WsHub.GetHub(userId)
	targetHub.send<- &SendRequest{
		userID: userId,
		data:   msg,
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