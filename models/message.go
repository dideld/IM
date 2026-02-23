package models

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
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
	TargetId uint //接收者
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
	Conn          *websocket.Conn //连接
	Addr          string          //客户端地址
	FirstTime     uint64          //首次连接时间
	HeartbeatTime uint64          //心跳时间
	LoginTime     uint64          //登录时间
	DataQueue     chan []byte     //消息
	GroupSets     set.Interface   //好友 / 群
}
//映射关系
var clientMap map[int64]*Node =make(map[int64]*Node,0)

//读写锁
var rwLock sync.RWMutex

func Chat(writer http.ResponseWriter,request *http.Request){
	//1.获取参数并校验合法性
	query:=request.URL.Query()
	Id:=query.Get("userId")
	userId,_:=strconv.ParseInt(Id,10,64)
	//token:=query.Get("token")
	//mtype:=query.Get("mtype")
	// targetId:=query.Get("targetId")
	// context:=query.Get("context")
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
	node:=&Node{
		Conn: conn,
		DataQueue: make(chan []byte,50),
		GroupSets:set.New(set.ThreadSafe),
	}
	//3.用户关系
	//4.userid和node绑定并加锁
	rwLock.Lock()
	clientMap[userId]=node
	rwLock.Unlock()

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
			fmt.Println("[ws]sendProc >>> msg",string(data))
			err:=node.Conn.WriteMessage(websocket.TextMessage,data)
			if err!=nil{
				fmt.Println(err)
				return 
			}
		}
	}
}
func recvProc(node *Node){
	for{
		_,data,err:=node.Conn.ReadMessage()
		if err!=nil{
			fmt.Println(err)
			return
		}
		broadMsg(data)
		fmt.Println("[ws] <<<<<",string(data))
	}
}
var udpsendChan chan []byte=make(chan []byte,1024)
func broadMsg(data []byte){
	udpsendChan <-data

}
func init(){
	go udpSendProc()
	go udpRevProc() 
	fmt.Println("init goroutine:------------")
}

//完成udp数据发送携程
func udpSendProc(){
	con,err:=net.DialUDP("udp",nil,&net.UDPAddr{
		IP: net.IPv4(192,168,115,1),
		Port: viper.GetInt("port.udp"),
	})
	
	if err!=nil{
		fmt.Println(err)
		return 
	}
	defer con.Close()
	for{
		select{
		case data:=<-udpsendChan:
			fmt.Println("udpSendProc  data :", string(data))
			_,err:=con.Write(data)
			if err!=nil{
				fmt.Println(err)
				return 
			}
		}
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
	for{
		var buf [512]byte
		n,err:=con.Read(buf[0:])
		if err!=nil{
			fmt.Println(err)
			return 
		}
		fmt.Println("udpRevProc  data :", string(buf[0:n]))
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
		fmt.Println("dispatch  data :", string(data))
		sendMsg(int64(msg.TargetId),data,)   //私信
	 case 2:
		sendGroupMsg(int64(msg.TargetId), data)   //群发
	// case 3:sendAllMsg()        //广播
	// case 4:                //其他

	}
}
func sendMsg(userId int64, msg []byte) {

	rwLock.RLock()
	node, ok := clientMap[userId]
	rwLock.RUnlock()

	jsonMsg := Message{}
	json.Unmarshal(msg, &jsonMsg)

	ctx := context.Background()
	targetIdStr := strconv.Itoa(int(userId))
	userIdStr := strconv.Itoa(int(jsonMsg.UserId))
	//设置时间戳
	jsonMsg.CreateTime = uint64(time.Now().Unix())
	r, err := utils.Red.Get(ctx, "online_"+userIdStr).Result()
	if err != nil {
		fmt.Println(err)
	}
	if r != "" {
		if ok {
			fmt.Println("sendMsg >>> userID: ", userId, "  msg:", string(msg))
			node.DataQueue <- msg
		}
	}
	var key string
	if userId > int64(jsonMsg.UserId) {
		key = "msg_" + userIdStr + "_" + targetIdStr
	} else {
		key = "msg_" + targetIdStr + "_" + userIdStr
	}
	res, err := utils.Red.ZRevRange(ctx, key, 0, -1).Result()
	if err != nil {
		fmt.Println(err)
	}
	score := float64(cap(res)) + 1
	ress, e := utils.Red.ZAdd(ctx, key, redis.Z{
    Score:  score,
    Member: msg,
}).Result() //jsonMsg
	//res, e := utils.Red.Do(ctx, "zadd", key, 1, jsonMsg).Result() //备用 后续拓展 记录完整msg
	if e != nil {
		fmt.Println(e)
	}
	fmt.Println(ress)
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
	//node, ok := clientMap[userIdA]
	//jsonMsg := Message{}
	//json.Unmarshal(msg, &jsonMsg)
	ctx := context.Background()
	userIdStr := strconv.Itoa(int(userIdA))
	targetIdStr := strconv.Itoa(int(userIdB))
	var key string
	if userIdA > userIdB {
		key = "msg_" + targetIdStr + "_" + userIdStr
	} else {
		key = "msg_" + userIdStr + "_" + targetIdStr
	}
	  exists, _ := utils.Red.Exists(ctx, key).Result()
    fmt.Printf("Key: %s, Exists: %v\n", key, exists)
    
    // 调试：查看集合大小
    size, _ := utils.Red.ZCard(ctx, key).Result()
    fmt.Printf("集合大小: %d\n", size)

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
	// 发送推送消息
	/**
	// 后台通过websoket 推送消息
	for _, val := range rels {
		fmt.Println("sendMsg >>> userID: ", userIdA, "  msg:", val)
		node.DataQueue <- []byte(val)
	}**/
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
func (node *Node) IsHeartbeatTimeOut(currentTime uint64) (timeout bool) {
	if node.HeartbeatTime+viper.GetUint64("timeout.HeartbeatMaxTime") <= currentTime {
		fmt.Println("心跳超时。。。自动下线", node)
		timeout = true
	}
	return
}
//清理超时连接
func CleanConnection(param interface{}) (result bool) {
	result = true
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("cleanConnection err", r)
		}
	}()
	//fmt.Println("定时任务,清理超时连接 ", param)
	//node.IsHeartbeatTimeOut()
	currentTime := uint64(time.Now().Unix())
	for i := range clientMap {
		node := clientMap[i]
		if node.IsHeartbeatTimeOut(currentTime) {
			fmt.Println("心跳超时..... 关闭连接：", node)
			node.Conn.Close()
		}
	}
	return result
}