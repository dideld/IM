package models

import (
	"sync/atomic"
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
	probe      chan *nodeProbe    // 只读探测：跨 goroutine 查 clients 的唯一合法通道（Run 内应答）
	nodeCount  atomic.Int64       // 本分片节点数：Run 内维护，供 monitor 无锁读（修复旧版外部遍历 map 的数据竞争）
}
const HubShardCount = 16 //分片数量

type SendRequest struct {
	userID int64
	data   []byte
}

// nodeProbe 探测请求：
//  - want=count：返回该用户"除 did 之外"的设备数（设备上限预检——同设备重连走替换，不该被上限挡）；
//  - want=online：是否有任何在线设备（离线推送判定）。
// clients map 只允许在所属分片 Run 内触碰，因此探测必须走 channel 请求-应答。
type nodeProbe struct {
	uid   int64
	did   string
	want  byte
	reply chan int
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
		probe:      make(chan *nodeProbe),
	}
}
func (h *Hub) Run() {
	maxDev := maxDevicesPerUser()
	for {
		select {

		case node := <-h.register:
			list := h.clients[node.UserId]
			kept := make([]*Node, 0, len(list)+1)
			replaced := false
			for _, n := range list {
				// 同设备重连（页面刷新）：摘除旧连接，一台设备只保留一个节点
				if n.DeviceId == node.DeviceId {
					h.dropNode(n)
					replaced = true
					continue
				}
				kept = append(kept, n)
			}
			// 硬上限兜底：预检(403)与注册之间的竞态窗口里可能挤进第 N+1 台，直接拒绝这条新连接。
			// close(DataQueue) 让 sendProc 退出并关 TCP；recvProc 读错误后 defer 清理（unregister 找不到指针即无害）。
			if !replaced && len(kept) >= maxDev {
				close(node.DataQueue)
				break
			}
			h.clients[node.UserId] = append(kept, node)
			h.nodeCount.Add(1)

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
			// Run 内天然单线程，读 clients 合法
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

const (
	probeCount  byte = 'c'
	probeOnline byte = 'o'
)

// GetHub 根据 UserId 获取对应的分片。
// 黄金比例混合再取高 4 位：对连续自增 id 与"低位有规律"的 id（步长 16 等）都均匀，
// 纯 `uid % 16` 在低位有规律的号段会把连接挤进同一分片。
func (m *HubManager) GetHub(userId int64) *Hub {
	index := int64((uint64(userId) * 0x9E3779B97F4A7C15) >> 60)
	return m.shards[index]
}

// DeviceCount 该用户在除 did 之外的在线设备数（设备上限预检用）。
// 探测超时返回 -1，调用方按"放行"处理——宁可多一台，不可在 Hub 繁忙时误拒新连接。
func (m *HubManager) DeviceCount(userId int64, did string) int {
	return m.askProbe(userId, did, probeCount, -1)
}

// HasOnline 该用户是否有任一在线设备（离线推送判定）。探测超时按"在线"处理：
// 宁可漏一次推送通知，也不在 Hub 繁忙时把在线用户误判离线引发推送风暴。
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

var WsHub = NewHubManager()

// 废弃原有的 O(N) 心跳轮询检测逻辑，改为由 TimeWheel 时间轮自动 O(1) 踢出

// ClientCount 在线节点总数：走各分片 Run 内维护的原子计数，外部不再遍历 clients（修数据竞争）。
func (h *HubManager) ClientCount() int {
	count := 0
	for _, shard := range h.shards {
		count += int(shard.nodeCount.Load())
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
			h.nodeCount.Add(-1)
			close(node.DataQueue)
			return
		}
	}
}
