// WS 压测 v3：go run test/bench_ws.go -mode echo|conn
//
// 服务端前置（勿把 dev 开关写回提交的 yml，用 env 注入，见 CLAUDE.md）：
//
//	AUTH_DEVSKIPWSTOKEN=true go run .
//	# echo/ack 档还要求 STORAGE_SKIPBENCHPERSIST=false（缺省即 false）——
//	# 管线已改为"先落盘再投递"，旁路档不落库→不分发→不回 ACK，只有 conn 档能跑。
//	# ack/echo 档数据会落开发库：跑完按 client_msg_id LIKE 'bench-%' 清 message 表；
//	# 跑前确认 Kafka LAG 近零，历史积压会把新消息的投递/ACK 淹没成"全丢包"假象。
//
// bench 启动时先试探拨一条连接，401/拒连直接报错提示缺哪个开关（仓库 yml 目前不含
// auth.devSkipWSToken，必须 env 注入，否则所有连接握手全量失败）。
//
// 三种模式（对齐野火 IM 阶段化压测思路：conn=连接承载、echo=分发+通知段、ack=发送落库回执全链路）：
//   -mode echo ：闭环回显（TargetId=自己；配平点是**落盘后分发的 own 消息帧**=接收方视角全往返）。
//     -wind 为在途窗口(1=一发一收)，QPS 是"测量结果"而非"参数乘积"——v2.1 开环下
//     发送数恒等于 clients×rate，服务端再慢数字也照样上万，此坑已修。
//     超时未回按 -timeout 判丢并计入丢包率、不进时延分位（修幸存者偏差）；
//     接收帧按 clientMsgId 严格配平：匹配/重复/非业务帧三类分桶，Type=4/5 事件
//     与欢迎帧只进 unexpected 计数，不再灌进"接收 QPS"。
//     注意：管线翻转后分发挂在落盘之后，skipBenchPersist=true 旁路档 echo 收不到帧。
//   -mode conn ：只测长连接承载（仅 Type=3 心跳、零业务消息）：拨号时延直方图、
//     掉线归因(read/write)、单连接成本。判据：持续 ≥2×timeout.DelayHeartbeat(120s)
//     online 不衰减才算过。
//   -mode ack  ：全链路闭环（野火"发送阶段"）：配平的帧是服务端 Type=6 ACK——
//     落库成功且放大分发完成后才回，RTT=发送→落库→回执 全链路时延。
//     必须 STORAGE_SKIPBENCHPERSIST=false 跑（该开关旁路时服务端永不回 ACK，超时全判丢）；
//     数据会落开发库，跑完按 client_msg_id LIKE 'bench-%' 定向清理 message 表与 p_* 会话键。
//     -timeout 建议 ≥3000ms（Kafka 同步往返 + 攒批 50ms + 批量 INSERT），ack 档 TPS 显著
//     低于 echo 档是预期——那才是"全链路容量"（单分区+单消费者是分母），echo 只测网络+分发段。
//     服务端应用层重投队列超限时会回 ACK Status=-1，单独计入"拒收"，不算完成。
//
// 容量判读 SLO：丢包率<0.1% 且 RTT P99<100ms 的档位下，最大"完成TPS"才是容量。
// 推荐矩阵（打流端与服务同机时固定同参数，结果才可比）：
//
//	clients ∈ {500,2k,5k,10k} × wind ∈ {1,4,16}，每档 -duration 180 取中段
//	go run test/bench_ws.go -mode echo -clients 5000 -wind 4 -duration 180
//	go run test/bench_ws.go -mode conn -clients 10000 -duration 300
//
// 真值核对（客户端"在线"是自报，必须对照服务端侧）：
//
//	netstat -an | grep ":8081" | grep ESTABLISHED | wc -l        # Linux/WSL
//	curl -s "http://127.0.0.1:8081/debug/pprof/goroutine?debug=1" | head -1
//	RSS增量/clients ≈ 单连接成本（DataQueue 1024 槽 chan + gorilla 读写缓冲 + 每连接 2 goroutine，
//	预期 35~50KB/连接；conn 模式的产出之一就是实测这组数）
//
// 已知限制：
//  1. Windows 单机对同一目标拨号受动态端口池(约16k)上限，>1.5w 连接请从 WSL/Linux 打流，
//     否则失败是打流端端口耗尽，不是服务容量。
//  2. unexpected 含建连期欢迎帧(≈每连接1条)属正常；全链路(skipBenchPersist=false)
//     下 Type=4 未读事件也进此桶。
//  3. 心跳携带 token="test"，服务端 TouchToken 会对不存在的 key 做两次 Redis EXPIRE，
//     属 dev 噪声；要测真实鉴权负载需先登录拿真 token（下阶段）。
//  4. 未覆盖：落库闭环(需服务端 Type=6 ack 帧或采样 /user/historyMsg)、ring 跨分片分发
//     与群聊 1→N 扇出、拉取接口(redisMsg/markRead)并发、多机打流。
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

var (
	modeF       = flag.String("mode", "echo", "压测模式：echo=闭环自回显，conn=长连接承载(只心跳)，ack=全链路(配平 Type=6 ACK)")
	clientsN    = flag.Int("clients", 5000, "总连接数")
	concurrency = flag.Int("concurrency", 200, "拨号并发(信号量；初始加压与重连共用)")
	host        = flag.String("host", "127.0.0.1:8081", "目标服务地址")
	start       = flag.Int("start", 1, "压测 uid 号段基(避开真实在线用户可调大)")
	hbSec       = flag.Int("hb", 10, "Type=3 心跳间隔秒(须小于服务端 timeout.DelayHeartbeat)")
	windF       = flag.Int("wind", 1, "echo 模式在途窗口(1=一发一收严格闭环)")
	timeoutMs   = flag.Int("timeout", 2000, "单条消息往返超时(ms)，超时判丢并释放窗口")
	durationSec = flag.Int("duration", 0, "运行 N 秒后自动收尾出总评(0=直到 Ctrl+C)")
)

const probeUID = 999999001 // 试探拨专用 uid，远离压测号段

var ackMode bool // -mode ack：以 Type=6 ACK 为配平点（全链路）

var (
	online, dialOK, dialFail int64
	// 建连成功后中途掉线的归因计数（不含建连失败；重连前计数）
	droppedRead, droppedWrite int64
	// attempted=发出的业务帧；echoOK=按 clientMsgId 配平成功的往返（echo 档=投递帧，ack 档=Type6 成功帧）；
	// lost=超时判丢（含 ack 档 Status=-1 显式拒收，拒收另记 rejected）；dup=重复配平帧
	attempted, echoOK, lost, dup, unexpected int64
	rejected, ackSeen, peerCopy int64
	inflightG               atomic.Int64 // 全体连接在途合计（增长=服务端消化不动的信号）
	stopped                 atomic.Bool
	stopCh                  = make(chan struct{})
	startTime               = time.Now()
	dialSem                 chan struct{}

	rttHist  = newHist([]int64{1, 2, 5, 10, 25, 50, 100, 200, 500, 1000})
	dialHist = newHist([]int64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000})
)

// ===== 直方图（桶近似分位数，零锁竞争）=====
type hist struct {
	bounds  []int64
	buckets []atomic.Int64
	count   atomic.Int64
	sumNs   atomic.Int64
	maxNs   atomic.Int64
}

func newHist(bounds []int64) *hist {
	h := &hist{bounds: bounds}
	h.buckets = make([]atomic.Int64, len(bounds)+1)
	return h
}

func (h *hist) record(d time.Duration) {
	h.count.Add(1)
	h.sumNs.Add(d.Nanoseconds())
	for {
		old := h.maxNs.Load()
		if d.Nanoseconds() <= old || h.maxNs.CompareAndSwap(old, d.Nanoseconds()) {
			break
		}
	}
	idx := sort.Search(len(h.bounds), func(i int) bool { return d.Milliseconds() <= h.bounds[i] })
	h.buckets[idx].Add(1)
}

func (h *hist) pct(p float64) string {
	total := h.count.Load()
	if total == 0 {
		return "-"
	}
	target := int64(float64(total) * p)
	var cum int64
	for i := range h.buckets {
		cum += h.buckets[i].Load()
		if cum >= target {
			if i == len(h.buckets)-1 {
				return fmt.Sprintf("%.0fms", float64(h.maxNs.Load())/1e6)
			}
			return fmt.Sprintf("≈%dms", h.bounds[i])
		}
	}
	return fmt.Sprintf("%.0fms", float64(h.maxNs.Load())/1e6)
}

func (h *hist) brief() string {
	if h.count.Load() == 0 {
		return "样本0"
	}
	mean := float64(h.sumNs.Load()) / float64(h.count.Load()) / 1e6
	return fmt.Sprintf("n=%d 均值:%.1fms P50:%s P90:%s P99:%s Max:%s",
		h.count.Load(), mean, h.pct(0.5), h.pct(0.9), h.pct(0.99),
		fmt.Sprintf("%.0fms", float64(h.maxNs.Load())/1e6))
}

// ===== 每连接状态（跨重连保持 seq 单调，保证 clientMsgId 全局唯一）=====
type connState struct {
	uid      int64
	uidStr   string
	idPrefix string // "bench-<uid>-"
	seq      int64
}

func main() {
	flag.Parse()
	if *modeF != "echo" && *modeF != "conn" && *modeF != "ack" {
		log.Fatalf("-mode 只支持 echo|conn|ack，收到:%q", *modeF)
	}
	ackMode = *modeF == "ack"
	W := 0
	if *modeF == "echo" || ackMode {
		W = max(1, *windF)
	}
	dialSem = make(chan struct{}, max(1, *concurrency))

	log.Printf("=== 压测启动 | 模式:%s 连接:%d 窗口:%d 心跳:%ds 超时:%dms 建连并发:%d 目标:%s | uid %d~%d ===",
		*modeF, *clientsN, W, *hbSec, *timeoutMs, *concurrency, *host, *start, *start+*clientsN-1)

	probe()
	go monitor(W)

	for i := 1; i <= *clientsN; i++ {
		st := &connState{
			uid:      int64(*start + i - 1),
			uidStr:   strconv.Itoa(*start + i - 1),
			idPrefix: fmt.Sprintf("bench-%d-", *start+i-1),
		}
		go supervise(st, W)
	}

	if *durationSec > 0 {
		go func() {
			time.Sleep(time.Duration(*durationSec) * time.Second)
			shutdown()
		}()
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	shutdown()
}

func shutdown() {
	if !stopped.CompareAndSwap(false, true) {
		return
	}
	close(stopCh)
	summary()
	os.Exit(0)
}

// probe 试探拨一条连接：把"开关没开/服务没起"这类全局性失败在启动期就暴露，
// 不再出现跑满一分钟才发现 failed 全部来自 401 的情况。
func probe() {
	st := &connState{uid: probeUID, uidStr: strconv.Itoa(probeUID)}
	conn, err := dialThrottled(st)
	if err != nil {
		if handshakeStatus(err) == 401 {
			log.Fatalf("[试探拨] 服务端 401：auth.devSkipWSToken 未开启。请用 "+
				"AUTH_DEVSKIPWSTOKEN=true go run . 启动服务（echo/ack 档还须 STORAGE_SKIPBENCHPERSIST=false）")
		}
		log.Fatalf("[试探拨] 无法连接 %s：%v（服务端是否已启动？）", *host, err)
	}
	conn.Close()
	log.Printf("[试探拨] 握手正常，开始加压。判读 SLO：丢包<0.1%% 且 P99<100ms 档位下的最大完成TPS 即容量")
}

// dialThrottled 全局信号量节流的拨号（初始加压与重连风暴共用）
func dialThrottled(st *connState) (*websocket.Conn, error) {
	dialSem <- struct{}{}
	defer func() { <-dialSem }()
	u := url.URL{Scheme: "ws", Host: *host, Path: "/chat"}
	q := u.Query()
	q.Set("userId", st.uidStr)
	q.Set("token", "test")
	q.Set("deviceId", st.uidStr+"-bench")
	u.RawQuery = q.Encode()

	t0 := time.Now()
	conn, resp, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		atomic.AddInt64(&dialFail, 1)
		status := 0
		if resp != nil { // gorilla 的 ErrBadHandshake 是哨兵值不带状态码，真值在 resp 里
			status = resp.StatusCode
		}
		return nil, &dialError{status: status, err: err}
	}
	dialHist.record(time.Since(t0))
	atomic.AddInt64(&dialOK, 1)
	return conn, nil
}

// dialError 包装拨号失败，携带握手状态码（401=鉴权开关没开的核心信号）
type dialError struct {
	status int
	err    error
}

func (d *dialError) Error() string {
	return fmt.Sprintf("dial status=%d: %v", d.status, d.err)
}

func (d *dialError) Unwrap() error { return d.err }

func handshakeStatus(err error) int {
	var de *dialError
	if errors.As(err, &de) {
		return de.status
	}
	return 0
}

// supervise 单连接监督循环：任何失败都指数退避重连，保证压测期间目标连接数恒定。
// 活过 10s 视为一次成功保活、退避复位——区分"服务端踢"与"打流端自杀"看 dropped*。
func supervise(st *connState, W int) {
	backoff := 500 * time.Millisecond
	for !stopped.Load() {
		conn, err := dialThrottled(st)
		alive := time.Duration(0)
		if err != nil {
			if handshakeStatus(err) == 401 {
				log.Fatalf("[运行期] 服务端 401（auth.devSkipWSToken 被关了？）：%v", err)
			}
		} else {
			t0 := time.Now()
			switch runLive(conn, st, W) {
			case "read":
				atomic.AddInt64(&droppedRead, 1)
			case "write":
				atomic.AddInt64(&droppedWrite, 1)
			}
			alive = time.Since(t0)
		}
		if err == nil && alive > 10*time.Second {
			backoff = 500 * time.Millisecond
		} else if backoff < 5*time.Second {
			backoff *= 2 // 封顶 5s：再长会漏心跳被时间轮踢，更短会在服务宕机时打空
		}
		jittered := backoff + time.Duration(rand.Int63n(int64(backoff/2)+1))
		select {
		case <-time.After(jittered):
		case <-stopCh:
			return
		}
	}
}

// runLive 一条连接的完整生命周期；返回掉线归因 read/write。
// 所有写(消息+心跳)都发生在本函数所在 goroutine——gorilla 禁止并发写。
func runLive(conn *websocket.Conn, st *connState, W int) string {
	atomic.AddInt64(&online, 1)
	defer atomic.AddInt64(&online, -1)
	defer conn.Close()

	var L sync.Mutex
	inflight := make(map[int64]time.Time, W+1)
	defer func() { // 本连接死亡时在途全清，归还在途 gauge（未回执的不再等待）
		L.Lock()
		n := len(inflight)
		for k := range inflight {
			delete(inflight, k)
		}
		L.Unlock()
		inflightG.Add(int64(-n))
	}()

	wake := make(chan struct{}, 4)
	readDone := make(chan struct{})
	var closeOnce sync.Once
	hbPayload := []byte(`{"UserId":` + st.uidStr + `,"Type":3,"Media":-1,"Content":"心跳"}`)

	// 读协程：只做配平计数，不回写
	go func() {
		defer closeOnce.Do(func() { close(readDone); conn.Close() })
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var f struct {
				ClientMsgId string `json:"clientMsgId"`
				SrvClient   string `json:"ClientMsgId"` // Type=6 ACK 是 Go 侧 marshal，Pascal 键
				Type        int    `json:"Type"`
				Status      int    `json:"Status"`
			}
			if json.Unmarshal(data, &f) != nil {
				atomic.AddInt64(&unexpected, 1) // 欢迎帧等非 JSON
				continue
			}
			cid := f.ClientMsgId
			if cid == "" {
				cid = f.SrvClient
			}
			if !strings.HasPrefix(cid, st.idPrefix) {
				atomic.AddInt64(&unexpected, 1) // Type=4/5 事件等，不进往返统计
				continue
			}
			seq, e := strconv.ParseInt(strings.TrimPrefix(cid, st.idPrefix), 10, 64)
			if e != nil {
				atomic.AddInt64(&unexpected, 1)
				continue
			}
			isAck := f.Type == 6
			if isAck != ackMode { // 另一类帧单独计数：ack 档的自投递副本 / echo 档的服务端回执
				if isAck {
					atomic.AddInt64(&ackSeen, 1)
				} else {
					atomic.AddInt64(&peerCopy, 1)
				}
				continue
			}
			L.Lock()
			ts, ok := inflight[seq]
			if ok {
				delete(inflight, seq)
			}
			L.Unlock()
			if !ok {
				atomic.AddInt64(&dup, 1)
				continue
			}
			inflightG.Add(-1)
			if isAck && f.Status != 0 { // 服务端显式拒收：计丢不计延（不进时延分位）
				atomic.AddInt64(&rejected, 1)
				atomic.AddInt64(&lost, 1)
			} else {
				atomic.AddInt64(&echoOK, 1)
				rttHist.record(time.Since(ts))
			}
			select {
			case wake <- struct{}{}:
			default:
			}
		}
	}()

	hbInterval := time.Duration(*hbSec) * time.Second
	if hbInterval <= 0 {
		hbInterval = 10 * time.Second
	}
	hb := time.NewTicker(hbInterval)
	defer hb.Stop()
	sweep := time.NewTicker(200 * time.Millisecond)
	defer sweep.Stop()
	timeout := time.Duration(*timeoutMs) * time.Millisecond

	for {
		// 补满在途窗口（conn 模式 W==0，循环体不进入，只发心跳）
		for W > 0 {
			L.Lock()
			if len(inflight) >= W {
				L.Unlock()
				break
			}
			st.seq++
			seq := st.seq
			now := time.Now()
			inflight[seq] = now
			L.Unlock()

			payload := `{"UserId":` + st.uidStr + `,"TargetId":` + st.uidStr +
				`,"Type":1,"Media":1,"Content":"bench|` + strconv.FormatInt(now.UnixNano(), 10) +
				`","clientMsgId":"` + st.idPrefix + strconv.FormatInt(seq, 10) + `"}`
			inflightG.Add(1)
			atomic.AddInt64(&attempted, 1)
			if err := conn.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
				return "write"
			}
		}

		select {
		case <-readDone:
			return "read"
		case <-wake:
			// 有回执释放了窗口，回循环补发
		case <-hb.C:
			if err := conn.WriteMessage(websocket.TextMessage, hbPayload); err != nil {
				return "write"
			}
		case <-sweep.C:
			if W == 0 {
				continue
			}
			L.Lock()
			for seq, ts := range inflight {
				if time.Since(ts) > timeout {
					delete(inflight, seq)
					inflightG.Add(-1)
					atomic.AddInt64(&lost, 1) // 丢包单独成率，不进时延分位
				}
			}
			L.Unlock()
		}
	}
}

func monitor(W int) {
	var ls, le, ld int64
	tick := 0
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		tick++
		e, s, d := atomic.LoadInt64(&echoOK), atomic.LoadInt64(&attempted), atomic.LoadInt64(&dialOK)
		line := fmt.Sprintf("[在线:%d/%d 建连+%d/s 建连失败累计:%d 掉线R/W:%d/%d 非业务帧:%d]",
			atomic.LoadInt64(&online), *clientsN, (d-ld)/2,
			atomic.LoadInt64(&dialFail),
			atomic.LoadInt64(&droppedRead), atomic.LoadInt64(&droppedWrite),
			atomic.LoadInt64(&unexpected))
		if W > 0 {
			l := atomic.LoadInt64(&lost)
			lossRate := 0.0
			if e+l > 0 {
				lossRate = float64(l) / float64(e+l) * 100
			}
			name := "回显"
			if ackMode {
				name = "ACK"
			}
			line += fmt.Sprintf(" [%s完成:%d/s 尝试:%d/s 在途:%d 丢包累计:%d(%.3f%%, 含拒收%d) dup:%d 旁帧:%d]",
				name, (e-le)/2, (s-ls)/2, inflightG.Load(), l, lossRate,
				atomic.LoadInt64(&rejected), atomic.LoadInt64(&dup),
				atomic.LoadInt64(&ackSeen)+atomic.LoadInt64(&peerCopy))
		}
		if tick%5 == 0 { // 每 10s 附带直方图摘要
			line += "\n        往返 " + rttHist.brief() + " | 拨号 " + dialHist.brief()
		}
		ls, le, ld = s, e, d
		log.Println(line)
	}
}

func summary() {
	e := atomic.LoadInt64(&echoOK)
	a := atomic.LoadInt64(&attempted)
	l := atomic.LoadInt64(&lost)
	dur := time.Since(startTime).Seconds()
	log.Println("========== 总评 ==========")
	log.Printf("模式:%s 运行:%.0fs 完成往返:%d(平均%.0f/s) 尝试:%d 丢包:%d 重复:%d 非业务帧:%d",
		*modeF, dur, e, float64(e)/dur, a, l,
		atomic.LoadInt64(&dup), atomic.LoadInt64(&unexpected))
	if e+l > 0 {
		log.Printf("丢包率:%.3f%% | 往返 %s", float64(l)/float64(e+l)*100, rttHist.brief())
	}
	log.Printf("建连成功:%d 失败:%d 中途掉线(读/写):%d/%d | 拨号 %s",
		atomic.LoadInt64(&dialOK), atomic.LoadInt64(&dialFail),
		atomic.LoadInt64(&droppedRead), atomic.LoadInt64(&droppedWrite), dialHist.brief())
	log.Printf("判读：满足 丢包<0.1%% 且 P99<100ms 的档位中最大完成TPS 才算容量；在线数请对 netstat :8081 ESTABLISHED 真值")
	if ackMode {
		log.Printf("ack 档判读：RTT 含 落库+分发+ACK 全链路（echo 档 P99<100ms 的 SLO 不适用，建议看 3s 超时下的丢包率）；")
		log.Printf("确认 STORAGE_SKIPBENCHPERSIST=false，否则永不回 ACK；压测数据按 client_msg_id LIKE 'bench-%%' 清理")
	} else {
		log.Printf("全链路另测：-mode ack（配平 Type=6，需 skipBenchPersist=false），观察完成TPS 与 Kafka LAG——WS 侧 echo 数字不反映落库能力")
	}
}
