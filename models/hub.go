package models

import (
	//"fmt"
	//"time"
	//"github.com/spf13/viper"
)
type Hub struct {
	clients    map[int64]*Node
	register   chan *Node
	unregister chan *Node
	send       chan *SendRequest
}
const HubShardCount = 16 //分片数量

type SendRequest struct {
	userID int64
	data   []byte
}
type HubManager struct {
    shards []*Hub
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
					h.closeNode(node)
				}
			}
		}
	}
}
func NewHubManager() *HubManager {
    m := &HubManager{
        shards: make([]*Hub, HubShardCount),
    }
    for i := 0; i < HubShardCount; i++ {
        m.shards[i] = NewHub() // 初始化每一个分片
        go m.shards[i].Run()    // 每个分片各跑各的 Goroutine
    }
    return m
}

// GetHub 根据 UserId 获取对应的分片
func (m *HubManager) GetHub(userId int64) *Hub {
    // 简单的求模算法
    index := userId % int64(HubShardCount)
    return m.shards[index]
}

var WsHub = NewHubManager()

// 废弃原有的 O(N) 心跳轮询检测逻辑，改为由 TimeWheel 时间轮自动 O(1) 踢出

func (h *HubManager) ClientCount() int {
	count := 0
    for _, shard := range h.shards {
        count += len(shard.clients)
    }
    return count
}
func (h *Hub)closeNode(node *Node){
	close(node.DataQueue)
	delete(h.clients, node.UserId)
}