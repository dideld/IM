package models

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"gopkg.in/fatih/set.v0"
	"github.com/gorilla/websocket"
	"gorm.io/gorm"
)

type Message struct {
	gorm.Model
	UserId uint //发送者
	TargetId uint //接收者
	Type int //群聊、私聊、广播
	Media int //消息类型  文字、表情包、图片、音频
	Content string //消息内容
	Pic string 
	Url string
	Desc string
	Amoubt int //其他数字统计

}
func (table *Message)TableName()string{
	return "message"
}

type Node struct{
	Conn *websocket.Conn
	DataQueue chan []byte
	GroupSets set.Interface
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
		Port:3000,
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
		Port:3000,

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
	// case 2:sendGroupMsg()   //群发
	// case 3:sendAllMsg()        //广播
	// case 4:                //其他

	}
}
func sendMsg(userId int64,msg []byte){
	rwLock.RLock()
	node,ok:=clientMap[userId]
	defer rwLock.RUnlock()
	if ok{
		node.DataQueue<-msg
	}
}