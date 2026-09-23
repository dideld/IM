// WS 压测：go run test/bench_ws.go
// 前置：P0 起 /chat 握手校验会话 token——压测请在 config/app.yml 设 auth.devSkipWSToken: true
// （dev-only 开关，跳过 WS 鉴权，生产严禁开启），或为压测账号批量签发真实 token。
package main

import (
	"log"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var (
	successConn int64
	failedConn  int64
	msgSent     int64
	msgReceived int64
)

func main() {
	totalClients := 20000      // 目标压测总连接数
	dialConcurrency := 200     // 建连并发度 (测试系统瞬时抗压/握手能力)
	messageRatePerSec := 8     // 单个用户每秒发多少条消息 (用于测出真实 QPS 带宽)

	log.Printf("=== 启动高并发压测 | 目标连接:%d | 预计总QPS: %d ===", totalClients, totalClients*messageRatePerSec)

	// 1. 启动独立协程，每2秒打印真实的 QPS 性能指标
	go monitorQPS()

	// 2. 利用 Worker 协程池执行瞬时海量并发建连
	var wg sync.WaitGroup
	wg.Add(totalClients)

	taskChan := make(chan int, totalClients)
	for i := 1; i <= totalClients; i++ {
		taskChan <- i
	}
	close(taskChan)

	startDialTime := time.Now()
	for w := 0; w < dialConcurrency; w++ {
		go func() {
			for i := range taskChan {
				connect(i, messageRatePerSec)
				wg.Done()
			}
		}()
	}

	wg.Wait() // 等待所有连接建立（或失败）完毕
	log.Printf("=== 压测群已100%%上线！建连耗时: %v ===", time.Since(startDialTime))

	// 3. 阻塞主协程，让压测子协程持续轰炸服务器
	select {}
}

func connect(userId int, rate int) {
	// 【注意】请确保这个 Path 是你目前真实的群聊或私聊接口地址
	u := url.URL{Scheme: "ws", Host: "127.0.0.1:8081", Path: "/chat"}
	q := u.Query()
	q.Set("userId", strconv.Itoa(userId))
	q.Set("token", "test")
	q.Set("deviceId", strconv.Itoa(userId)+"-bench") //每连接唯一设备号，适配多端在线 Hub
	u.RawQuery = q.Encode()

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		atomic.AddInt64(&failedConn, 1)
		return
	}
	atomic.AddInt64(&successConn, 1)

	// 持续接收消息协程
	go func() {
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				conn.Close()
				return
			}
			atomic.AddInt64(&msgReceived, 1) // 记录成功接收的总包数
		}
	}()

	// 持续发送消息测压协程
	go func() {
		// 定时器控制发送频率
		ticker := time.NewTicker(time.Second / time.Duration(rate))
		defer ticker.Stop()

		for range ticker.C {
			// Type 1：私聊发给“自己”。这能产生一个完整的处理周期：
			// 客户端 -> Ws服务器 -> 解析 -> 派发逻辑 -> Redis/DB落盘 -> Ws发回客户端自身
			msg := `{"Type":1, "TargetId":` + strconv.Itoa(userId) + `, "Content":"bench message"}`
			
			err := conn.WriteMessage(websocket.TextMessage, []byte(msg))
			if err != nil {
				return
			}
			atomic.AddInt64(&msgSent, 1) // 记录成功发出的总包数
		}
	}()
}

// QPS 计算面板
func monitorQPS() {
	var lastSent, lastRecv int64
	ticker := time.NewTicker(2 * time.Second)
	for range ticker.C {
		currentSent := atomic.LoadInt64(&msgSent)
		currentRecv := atomic.LoadInt64(&msgReceived)

		// 除以2是因为定时器2秒统计一次差值
		qpsSent := (currentSent - lastSent) / 2
		qpsRecv := (currentRecv - lastRecv) / 2

		log.Printf("[实时监控] 在线人:%d | 失败:%d | 发送 QPS: %d 次/秒 | 接收 QPS: %d 次/秒 \n",
			atomic.LoadInt64(&successConn),
			atomic.LoadInt64(&failedConn),
			qpsSent,
			qpsRecv,
		)

		lastSent = currentSent
		lastRecv = currentRecv
	}
}
