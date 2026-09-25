// WS 压测 v2：go run test/bench_ws.go
//
// 服务端前置（config/app.yml，压测完记得关）：
//
//	auth.devSkipWSToken: true       # 跳过 WS 会话鉴权（dev-only）
//	storage.skipBenchPersist: true  # bench- 前缀消息跳过冷热存储，避免污染开发库
//
// 参数：-clients 总连接数 -rate 每连接每秒消息 -concurrency 建连并发 -host 目标地址
// 例：go run test/bench_ws.go -clients 5000 -rate 4
//
// v2 相比 v1 的关键修正（v1 的数字不可信）：
//  1. 携带 Type=3 心跳（v1 没有 → 60s 后被服务端时间轮全量踢线，之后统计全是假数）
//  2. 发送帧补 UserId（v1 缺 → 落库侧生成 p_0_{uid} 垃圾会话）
//  3. 端到端时延直方图（内存分发路径 P50/P90/P99/Max；持久化滞后看 Kafka lag，不在返回路径上）
//  4. clientMsgId 带 bench- 前缀，配合 skipBenchPersist 开关
//  5. 断连即时减在线数（v1 只加不减，"在线2万"虚报）
//
// 仍未覆盖（做全量报告前须知）：群聊/跨用户扇出混合流量、慢消费者单端被踢专项、多机打流。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var (
	clientsN    = flag.Int("clients", 20000, "总连接数")
	rate        = flag.Int("rate", 8, "每连接每秒消息数")
	concurrency = flag.Int("concurrency", 200, "建连并发度")
	host        = flag.String("host", "127.0.0.1:8081", "目标服务地址")
)

var (
	online, failed, msgSent, msgRecv int64
)

// ===== 时延直方图（近似分位数，零锁竞争）=====
var latBoundsMs = []int64{1, 2, 5, 10, 25, 50, 100, 200, 500, 1000}
var latBuckets [11]atomic.Int64 // 最后一桶 = >1000ms
var latCount, latSumNs, latMaxNs atomic.Int64

func recordLat(d time.Duration) {
	latCount.Add(1)
	latSumNs.Add(d.Nanoseconds())
	for {
		old := latMaxNs.Load()
		if d.Nanoseconds() <= old || latMaxNs.CompareAndSwap(old, d.Nanoseconds()) {
			break
		}
	}
	idx := sort.Search(len(latBoundsMs), func(i int) bool { return d.Milliseconds() <= latBoundsMs[i] })
	latBuckets[idx].Add(1)
}

// approxPercentile 返回 ≈p 分位的毫秒上界（桶近似）
func approxPercentile(p float64) string {
	total := latCount.Load()
	if total == 0 {
		return "-"
	}
	target := int64(float64(total) * p)
	var cum int64
	for i := range latBuckets {
		cum += latBuckets[i].Load()
		if cum >= target {
			if i == len(latBuckets)-1 {
				return ">1000ms"
			}
			return fmt.Sprintf("≈%dms", latBoundsMs[i])
		}
	}
	return "-"
}

func main() {
	flag.Parse()
	log.Printf("=== 压测启动 | 连接:%d 每连接QPS:%d 并发建连:%d 目标:%s ===", *clientsN, *rate, *concurrency, *host)
	log.Println("服务端需开: auth.devSkipWSToken=true, storage.skipBenchPersist=true（详见文件头注释）")

	go monitor()

	var wg sync.WaitGroup
	wg.Add(*clientsN)
	taskChan := make(chan int, *clientsN)
	for i := 1; i <= *clientsN; i++ {
		taskChan <- i
	}
	close(taskChan)

	startDial := time.Now()
	for w := 0; w < *concurrency; w++ {
		go func() {
			for i := range taskChan {
				connect(i)
				wg.Done()
			}
		}()
	}
	wg.Wait()
	log.Printf("=== 建连阶段结束(成功:%d 失败:%d) 耗时:%v，持续加压中(Ctrl+C 退出) ===",
		atomic.LoadInt64(&online), atomic.LoadInt64(&failed), time.Since(startDial))
	select {}
}

func connect(userId int) {
	u := url.URL{Scheme: "ws", Host: *host, Path: "/chat"}
	q := u.Query()
	q.Set("userId", strconv.Itoa(userId))
	q.Set("token", "test")
	q.Set("deviceId", strconv.Itoa(userId)+"-bench")
	u.RawQuery = q.Encode()

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		atomic.AddInt64(&failed, 1)
		return
	}
	atomic.AddInt64(&online, 1)
	uidStr := strconv.Itoa(userId)

	// 接收：识别自己 echo 的 bench 消息计算端到端时延
	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				atomic.AddInt64(&online, -1)
				conn.Close()
				return
			}
			atomic.AddInt64(&msgRecv, 1)
			if !strings.Contains(string(data), "bench|") {
				continue
			}
			var m struct {
				UserId  int64  `json:"UserId"`
				Content string `json:"Content"`
			}
			if json.Unmarshal(data, &m) == nil && strconv.FormatInt(m.UserId, 10) == uidStr {
				if ci := strings.Index(m.Content, "bench|"); ci >= 0 {
					if tail := m.Content[ci+len("bench|"):]; tail != "" {
						if nano, e := strconv.ParseInt(tail, 10, 64); e == nil {
							recordLat(time.Since(time.Unix(0, nano)))
						}
					}
				}
			}
		}
	}()

	// 单一发送协程：消息按 rate 频率发，每 rate*10 拍插发一条 Type=3 心跳
	// （gorilla 不允许并发写同一连接，心跳与消息必须同协程交替；v1 无心跳 60s 必掉线）
	go func() {
		ticker := time.NewTicker(time.Second / time.Duration(*rate))
		defer ticker.Stop()
		tick := 0
		for range ticker.C {
			tick++
			var payload string
			if tick%(*rate*10) == 0 {
				payload = `{"UserId":` + uidStr + `,"Type":3,"Media":-1,"Content":"心跳"}`
			} else {
				payload = `{"UserId":` + uidStr + `,"TargetId":` + uidStr +
					`,"Type":1,"Media":1,"Content":"bench|` + strconv.FormatInt(time.Now().UnixNano(), 10) +
					`","clientMsgId":"bench-` + uidStr + `-` + strconv.Itoa(tick) + `"}`
			}
			if err := conn.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
				return
			}
			if payload[0] != 0 && strings.Contains(payload, `"Type":1`) {
				atomic.AddInt64(&msgSent, 1)
			}
		}
	}()
}

func monitor() {
	var lastSent, lastRecv int64
	tick := 0
	ticker := time.NewTicker(2 * time.Second)
	for range ticker.C {
		tick++
		s, r := atomic.LoadInt64(&msgSent), atomic.LoadInt64(&msgRecv)
		line := fmt.Sprintf("[QPS] 在线:%d 失败:%d 发送:%d/s 接收:%d/s",
			atomic.LoadInt64(&online), atomic.LoadInt64(&failed), (s-lastSent)/2, (r-lastRecv)/2)
		lastSent, lastRecv = s, r
		if tick%5 == 0 && latCount.Load() > 0 { // 每 10s 附带时延摘要
			mean := float64(latSumNs.Load()) / float64(latCount.Load()) / 1e6
			line += fmt.Sprintf(" | 时延n=%d 均值:%.1fms P50:%s P90:%s P99:%s Max:%.0fms",
				latCount.Load(), mean, approxPercentile(0.5), approxPercentile(0.9), approxPercentile(0.99),
				float64(latMaxNs.Load())/1e6)
		}
		log.Println(line)
	}
}
