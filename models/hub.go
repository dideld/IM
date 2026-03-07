package models

import(
	"time"
	"fmt"
	"github.com/spf13/viper"	
)
type Hub struct {
	clients    map[int64]*Node
	register   chan *Node
	unregister chan *Node
	send       chan *SendRequest
}

type SendRequest struct {
	userID int64
	data   []byte
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[int64]*Node),
		register:   make(chan *Node),
		unregister: make(chan *Node),
		send:       make(chan *SendRequest, 1024),
	}
}
func (h *Hub) Run() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {

		case node := <-h.register:
			h.clients[node.UserId] = node

		case node := <-h.unregister:
			if _, ok := h.clients[node.UserId]; ok {
				delete(h.clients, node.UserId)
				close(node.DataQueue)
			}

		case req := <-h.send:
			if node, ok := h.clients[req.userID]; ok {
				select {
				case node.DataQueue <- req.data:
				default:
					close(node.DataQueue)
					delete(h.clients, node.UserId)
				}
			}
		case <-ticker.C:
			h.checkHeartbeat()
		}
	}
}
func (h *Hub) checkHeartbeat() {
	current := uint64(time.Now().Unix())

	for userID, node := range h.clients {
		if node.IsHeartbeatTimeOut(current) {
			fmt.Println("当前时间:", current, "心跳时间:", node.HeartbeatTime,"设置时间：",uint64(viper.GetInt("timeout.DelayHeartbeat")))
			fmt.Println("心跳超时，踢出:", userID)
			node.Conn.Close()
			delete(h.clients, userID)
		}
	}
}