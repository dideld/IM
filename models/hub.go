package models

import (
	"time"
	//"fmt"
	//"time"
	//"github.com/spf13/viper"
)

// 多端在线：clients 按用户维护设备节点列表（uid → []*Node），
// 一次投递扇出到该用户全部设备；背压/心跳只摘除单个设备节点，不影响其余端。
type Hub struct {
	clients    map[int64][]*Node
	register   chan *Node
	unregister chan *Node
	send       chan *SendRequest
	probe      chan *onlineProbe // 只读在线探测：跨 goroutine 查 clients 的唯一合法通道（Run 内应答）
}
const HubShardCount = 16 //分片数量

type SendRequest struct {
	userID int64
	data   []byte
}

// onlineProbe 离线推送挂点用：判断"该 uid 是否有任何在线设备"。
// clients map 只允许在所属分片 Run 内触碰（见 CLAUDE.md），因此探测必须走 channel 请求-应答。
type onlineProbe struct {
	uid   int64
	reply chan bool
}
type HubManager struct {
    shards []*Hub
}
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[int64][]*Node),
		register:   make(chan *Node),
		unregister: make(chan *Node),
		send:       make(chan *SendRequest, 1024),
		probe:      make(chan *onlineProbe),
	}
}
func (h *Hub) Run() {
	for {
		select {

		case node := <-h.register:
			list := h.clients[node.UserId]
			kept := make([]*Node, 0, len(list)+1)
			for _, n := range list {
				// 同设备重连（页面刷新）：摘除旧连接，一台设备只保留一个节点
				if n.DeviceId == node.DeviceId {
					h.dropNode(n)
					continue
				}
				kept = append(kept, n)
			}
			h.clients[node.UserId] = append(kept, node)

		case node := <-h.unregister:
			// 只摘自己：指针不匹配（已被替换/清理）时静默忽略，防误删其他端
			h.dropNode(node)

		case req := <-h.send:
			// 扇出到全部设备；某台设备队列满只踢它自己（慢端不拖累快端）
			for _, node := range h.clients[req.userID] {
				select {
				case node.DataQueue <- req.data:
				default:
					h.dropNode(node)
				}
			}

		case p := <-h.probe:
			// Run 内天然单线程，读 clients 合法（离线推送挂点专用）
			p.reply <- len(h.clients[p.uid]) > 0
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

// HasOnline 经 probe 通道询问分片"该用户是否有任一在线设备"（clients 只能在 Run 内触碰）。
// 探测超时按"在线"处理——宁可漏一次推送通知，也不在 Hub 繁忙时把在线用户误判离线引发推送风暴。
func (m *HubManager) HasOnline(userId int64) bool {
	hub := m.GetHub(userId)
	reply := make(chan bool, 1)
	probe := &onlineProbe{uid: userId, reply: reply}
	select {
	case hub.probe <- probe:
	case <-time.After(100 * time.Millisecond):
		return true
	}
	select {
	case ok := <-reply:
		return ok
	case <-time.After(200 * time.Millisecond):
		return true
	}
}

var WsHub = NewHubManager()

// 废弃原有的 O(N) 心跳轮询检测逻辑，改为由 TimeWheel 时间轮自动 O(1) 踢出

func (h *HubManager) ClientCount() int {
	count := 0
    for _, shard := range h.shards {
		for _, list := range shard.clients {
			count += len(list)
		}
    }
    return count
}

// dropNode 按指针身份摘除并关闭单个设备节点：
// 指针比对防止旧连接的延迟下线事件误删同设备重建的新连接（同 uid 多端时更不能按 uid 整删）。
// close(DataQueue) 后 sendProc 负责断开该端 TCP，让客户端感知被踢。
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
			close(node.DataQueue)
			return
		}
	}
}

