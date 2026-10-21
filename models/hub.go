package models

import (
	"sync/atomic"
	"time"
)

// Hub 多端在线：clients 按用户维护设备节点列表（uid → []*Node），
// 一次投递扇出到该用户全部设备；背压/心跳只摘除单个设备节点，不影响其余端。
type Hub struct {
	clients    map[int64][]*Node
	register   chan *Node
	unregister chan *Node
	send       chan *SendRequest
	probe      chan *nodeProbe
	nodeCount  atomic.Int64
}

// HubShardCount 分片数。
const HubShardCount = 16

type SendRequest struct {
	userID int64
	data   []byte
}

// nodeProbe 探测请求：want=count 返回除 did 外的设备数，want=online 返回是否有在线设备。
// clients map 只允许在所属分片 Run 内触碰，跨 goroutine 查询必须走 channel 请求-应答。
type nodeProbe struct {
	uid   int64
	did   string
	want  byte
	reply chan int
}

// HubManager 分片管理：每个 Hub 独立 goroutine，按 uid 哈希分片降低锁竞争。
type HubManager struct {
	shards []*Hub
}

// NewHub 创建单个 Hub 分片。
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[int64][]*Node),
		register:   make(chan *Node),
		unregister: make(chan *Node),
		send:       make(chan *SendRequest, 1024),
		probe:      make(chan *nodeProbe),
	}
}
// Run Hub 主循环：处理注册/注销/投递/探测四种事件。
func (h *Hub) Run() {
	maxDev := maxDevicesPerUser()
	for {
		select {

		// 注册：同设备重连替换旧节点，超上限拒绝新连接
		case node := <-h.register:
			list := h.clients[node.UserId]
			kept := make([]*Node, 0, len(list)+1)
			replaced := false
			for _, n := range list {
				if n.DeviceId == node.DeviceId {
					h.dropNode(n)
					replaced = true
					continue
				}
				kept = append(kept, n)
			}
			if !replaced && len(kept) >= maxDev {
				close(node.DataQueue)
				break
			}
			h.clients[node.UserId] = append(kept, node)
			h.nodeCount.Add(1)

		// 注销：按指针摘除，指针不匹配时静默忽略
		case node := <-h.unregister:
			h.dropNode(node)

		// 投递：扇出到全部设备，队列满只踢慢端
		case req := <-h.send:
			for _, node := range h.clients[req.userID] {
				select {
				case node.DataQueue <- req.data:
				default:
					h.dropNode(node)
				}
			}

		// 探测：Run 内单线程读 clients 安全
		case p := <-h.probe:
			n := 0
			for _, node := range h.clients[p.uid] {
				if p.want == probeCount {
					if node.DeviceId != p.did {
						n++
					}
				} else {
					n = 1
					break
				}
			}
			p.reply <- n
		}
	}
}

// NewHubManager 创建分片管理器并启动各分片 goroutine。
func NewHubManager() *HubManager {
	m := &HubManager{
		shards: make([]*Hub, HubShardCount),
	}
	for i := 0; i < HubShardCount; i++ {
		m.shards[i] = NewHub()
		go m.shards[i].Run()
	}
	return m
}

const (
	probeCount  byte = 'c'
	probeOnline byte = 'o'
)

// GetHub 按 uid 哈希取分片（黄金比例混合，对连续/有规律 id 均均匀）。
func (m *HubManager) GetHub(userId int64) *Hub {
	index := int64((uint64(userId) * 0x9E3779B97F4A7C15) >> 60)
	return m.shards[index]
}

// DeviceCount 该用户在除 did 之外的在线设备数。探测超时返回 -1（按放行处理）。
func (m *HubManager) DeviceCount(userId int64, did string) int {
	return m.askProbe(userId, did, probeCount, -1)
}

// HasOnline 该用户是否有任一在线设备。探测超时按在线处理（防漏推）。
func (m *HubManager) HasOnline(userId int64) bool {
	return m.askProbe(userId, "", probeOnline, 1) > 0
}

func (m *HubManager) askProbe(userId int64, did string, want byte, timeoutVal int) int {
	hub := m.GetHub(userId)
	reply := make(chan int, 1)
	p := &nodeProbe{uid: userId, did: did, want: want, reply: reply}
	select {
	case hub.probe <- p:
	case <-time.After(100 * time.Millisecond):
		return timeoutVal
	}
	select {
	case n := <-reply:
		return n
	case <-time.After(200 * time.Millisecond):
		return timeoutVal
	}
}

// WsHub 全局 HubManager 实例。
var WsHub = NewHubManager()

// ClientCount 在线节点总数：走各分片 Run 内维护的原子计数，外部无锁读。
func (h *HubManager) ClientCount() int {
	count := 0
	for _, shard := range h.shards {
		count += int(shard.nodeCount.Load())
	}
	return count
}

// dropNode 按指针身份摘除并关闭单个设备节点，close(DataQueue) 触发 sendProc 断开 TCP。
func (h *Hub) dropNode(node *Node) {
	list := h.clients[node.UserId]
	for i, n := range list {
		if n == node {
			list = append(list[:i], list[i+1:]...)
			if len(list) == 0 {
				delete(h.clients, node.UserId)
			} else {
				h.clients[node.UserId] = list
			}
			h.nodeCount.Add(-1)
			close(node.DataQueue)
			return
		}
	}
}
