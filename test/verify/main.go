// 管线回归验证工装（常驻入库，跑真实服务端的端到端断言）——改管线必跑。
//
// 前置（练手栈）：
//
//	docker-compose up -d redis kafka mysql
//	$env:MYSQL_DNS="root:111111@tcp(127.0.0.1:13306)/ginchat?..."; $env:REDIS_ADDR="127.0.0.1:6379"; $env:KAFKA_BROKER="127.0.0.1:9092"
//	$env:PUSH_GROUPPUSHMAXMEMBERS="4"; $env:TIMEOUT_DELAYHEARTBEAT="10"   # 让大群/多端用例可触发
//	go run .    # 服务端与被测工具须同一组 env（否则工具查询会连错库）
//
// 用法：go run ./test/verify          （建 vp% 测试用户/群，跑 S1-S6）
//
//	go run ./test/verify -clean  （清场；退出码非 0 = 有 FAIL）
//
// 断言覆盖：未读事件按(人,会话)聚合 / 小群常规扇出 / 大群 Type=7 合并帧+计算式未读 /
// 多端扇出+设备隔离踢线 / 同 clientMsgId 跨窗重发幂等 / 重放风暴取证（应 0 冷层冲突）。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"webapp/models"
	"webapp/utils"

	"github.com/gorilla/websocket"
	"github.com/spf13/viper"
)

var (
	host    = flag.String("host", "127.0.0.1:8081", "服务端地址")
	clean   = flag.Bool("clean", false, "清理 vp% 测试数据后退出")
	hbWait  = 16 * time.Second // 心跳踢线等待窗口（须 > TIMEOUT_DELAYHEARTBEAT=10，留调度抖动余量）
	failCnt int
	wmu     sync.Map // *websocket.Conn -> *sync.Mutex：gorilla 只禁"同一连接"并发写，跨连接并行无妨——
	//                旧版一把全局锁串行化所有连接的心跳+发送，多端用例下活跃端心跳被饿死致误踢（S4b 假失败）。
)

// wlock 取该连接专属写锁（LoadOrStore 保证同一 conn 复用同一把锁）
func wlock(c *websocket.Conn) *sync.Mutex {
	v, _ := wmu.LoadOrStore(c, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func check(name string, ok bool, detail ...interface{}) {
	if ok {
		fmt.Printf("PASS  %s\n", name)
	} else {
		failCnt++
		fmt.Printf("FAIL  %s  %v\n", name, detail)
	}
}

func skip(name string, why string) { fmt.Printf("SKIP  %s (%s)\n", name, why) }

type user struct {
	id    int64
	token string
	name  string
}

func postForm(path string, vals url.Values, token string) (map[string]interface{}, int) {
	req, _ := http.NewRequest("POST", "http://"+*host+path, strings.NewReader(vals.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Token", token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var m map[string]interface{}
	json.Unmarshal(body, &m)
	return m, resp.StatusCode
}

// 宽容响应键（历史契约大小写并存）
func gv(m map[string]interface{}, lower, upper string) interface{} {
	if v, ok := m[lower]; ok {
		return v
	}
	return m[upper]
}

func gi(v interface{}) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	}
	return -1
}

func rows(m map[string]interface{}) []interface{} {
	if v, ok := gv(m, "rows", "Rows").([]interface{}); ok {
		return v
	}
	if d, ok := gv(m, "data", "Data").(map[string]interface{}); ok {
		if l, ok := d["list"].([]interface{}); ok {
			return l
		}
	}
	return nil
}

func reg(name string) user {
	pwd := "pwd123456"
	postForm("/user/createUser", url.Values{"name": {name}, "password": {pwd}, "Identity": {pwd}}, "")
	m, _ := postForm("/user/findUserByNameAndPwd", url.Values{"name": {name}, "password": {pwd}}, "")
	d, _ := gv(m, "data", "Data").(map[string]interface{})
	if d == nil {
		panic("login failed " + name + " " + fmt.Sprint(m))
	}
	return user{id: gi(d["ID"]), token: fmt.Sprint(d["Identity"]), name: name}
}

var globalHbStop = make(chan struct{})

func dialOpt(u user, dev string, hb bool) (*websocket.Conn, int) {
	c, resp, err := websocket.DefaultDialer.Dial(
		fmt.Sprintf("ws://%s/chat?userId=%d&token=%s&deviceId=%s", *host, u.id, u.token, dev), nil)
	code := 0
	if resp != nil {
		code = resp.StatusCode
	}
	if err != nil {
		fmt.Println("dial fail:", u.name, err, code)
		return nil, code
	}
	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	c.ReadMessage() // 欢迎语
	if hb {
		hbKeep(c, u.id, globalHbStop)
	}
	return c, 200
}

func dial(u user, dev string) (*websocket.Conn, int) { return dialOpt(u, dev, true) }

func dialNoHb(u user, dev string) (*websocket.Conn, int) { return dialOpt(u, dev, false) }

func collect(c *websocket.Conn, d time.Duration) []map[string]interface{} {
	var out []map[string]interface{}
	if c == nil {
		return out
	}
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		c.SetReadDeadline(time.Now().Add(time.Until(deadline)))
		_, data, err := c.ReadMessage()
		if err != nil {
			break // gorilla: 读超时即弃连接
		}
		var m map[string]interface{}
		if json.Unmarshal(data, &m) == nil {
			out = append(out, m)
		}
	}
	return out
}

// probeRead 在窗口内读帧：命中含 token 的帧即 (true,nil) 早返回；
// 否则读到 deadline 或错误为止，返回 (false, 末次错误)——区分"仍在线但没这帧"(timeout)
// 与"已被服务端踢"(real drop)。关键：不能只读一帧——被踢设备缓冲区里可能残留踢前的旧帧，
// 单次 ReadMessage 会成功读到旧帧而误判"未踢"。
func probeRead(c *websocket.Conn, d time.Duration, token string) (bool, error) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		c.SetReadDeadline(time.Now().Add(time.Until(deadline)))
		_, data, err := c.ReadMessage()
		if err != nil {
			return false, err
		}
		if strings.Contains(string(data), token) {
			return true, nil
		}
	}
	return false, nil
}

func cntType(frames []map[string]interface{}, t int, pred func(map[string]interface{}) bool) int {
	n := 0
	for _, f := range frames {
		if ty, _ := f["Type"].(float64); int(ty) == t && (pred == nil || pred(f)) {
			n++
		}
	}
	return n
}

// 无 Type 字段的原文业务帧（客户端 JSON 是 "Type" 大写键——WS 转发的是原始帧）
func cntRaw(frames []map[string]interface{}, want string) int {
	n := 0
	for _, f := range frames {
		if c, _ := f["Content"].(string); strings.Contains(c, want) {
			if ty, ok := f["Type"].(float64); ok && (int(ty) == 1 || int(ty) == 2) {
				n++
			}
		}
	}
	return n
}

// hbKeep 长连接保活心跳：每 3s 一条 Type=3（服务端 DelayHeartbeat 配小后不心跳的测试连接必被踢）
func hbKeep(c *websocket.Conn, uid int64, stop <-chan struct{}) {
	go func() {
		p := `{"UserId":` + strconv.FormatInt(uid, 10) + `,"Type":3,"Media":-1,"Content":"心跳"}`
		t := time.NewTicker(3 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				l := wlock(c)
				l.Lock()
				err := c.WriteMessage(websocket.TextMessage, []byte(p))
				l.Unlock()
				if err != nil {
					return
				}
			case <-stop:
				return
			}
		}
	}()
}

func sendWS(c *websocket.Conn, from user, to int64, typ int, content, cmid string) {
	p := fmt.Sprintf(`{"UserId":%d,"TargetId":%d,"Type":%d,"Media":1,"Content":%q,"clientMsgId":%q}`, from.id, to, typ, content, cmid)
	l := wlock(c)
	l.Lock()
	defer l.Unlock()
	if err := c.WriteMessage(websocket.TextMessage, []byte(p)); err != nil {
		fmt.Println("SENDFAIL", err)
	}
}

func isRealDrop(err error) bool {
	return err != nil && !strings.Contains(err.Error(), "i/o timeout")
}

func mkGroup(owner user, name string, joiners ...user) int64 {
	postForm("/contact/createCommunity", url.Values{"name": {name}, "icon": {""}, "desc": {""}}, owner.token)
	lc, _ := postForm("/contact/loadCommunity", url.Values{}, owner.token)
	gid := int64(0)
	for _, it := range rows(lc) {
		g := it.(map[string]interface{})
		if g["Name"] == name {
			gid = gi(g["ID"])
		}
	}
	for _, j := range joiners {
		postForm("/contact/joinGroup", url.Values{"comId": {strconv.FormatInt(gid, 10)}}, j.token)
	}
	return gid
}

func unreadOf(u user, conv string) int64 {
	ul, _ := postForm("/user/unreadList", url.Values{}, u.token)
	dm, _ := gv(ul, "data", "Data").(map[string]interface{})
	if dm == nil {
		return -1
	}
	return gi(dm[conv])
}

func privConvOf(a, b int64) string {
	if a > b {
		a, b = b, a
	}
	return fmt.Sprintf("p_%d_%d", a, b)
}

func main() {
	flag.Parse()
	utils.InitConfig() // 与服务端同一 env 口径（宿主机直跑请注入 MYSQL_DNS/REDIS_ADDR）
	utils.InitMysql()
	utils.InitRedis()
	ctx := context.Background()

	if *clean {
		var ids []int64
		utils.DB.Model(&models.UserBasic{}).Where("name LIKE ?", "vp%").Pluck("id", &ids)
		if len(ids) > 0 {
			utils.DB.Where("user_id IN ? OR target_id IN ?", ids, ids).Unscoped().Delete(&models.Message{})
			utils.DB.Where("owner_id IN ? OR target_id IN ?", ids, ids).Unscoped().Delete(&models.Contact{})
			utils.DB.Where("user_id IN ?", ids).Unscoped().Delete(&models.ReadCursor{})
			utils.DB.Where("name LIKE ?", "vp%").Unscoped().Delete(&models.UserBasic{})
		}
		var gs []struct{ ID uint }
		utils.DB.Table("communities").Select("id").Where("name LIKE ?", "vp群%").Find(&gs)
		for _, g := range gs {
			gk := "g_" + strconv.FormatUint(uint64(g.ID), 10)
			utils.DB.Where("target_id = ?", g.ID).Unscoped().Delete(&models.Contact{})
			utils.DB.Where("conv_key = ?", gk).Unscoped().Delete(&models.Message{})
			utils.DB.Where("conv_key = ?", gk).Unscoped().Delete(&models.ReadCursor{})
		}
		utils.DB.Where("name LIKE ?", "vp群%").Unscoped().Delete(&models.Community{})
		keys, _ := utils.Red.Keys(ctx, "*").Result()
		var del []string
		for _, k := range keys {
			if strings.HasPrefix(k, "p_") || strings.HasPrefix(k, "g_") || strings.HasPrefix(k, "seq:") ||
				strings.HasPrefix(k, "cm:") || strings.HasPrefix(k, "unread:") || strings.HasPrefix(k, "read:") ||
				strings.HasPrefix(k, "g_members") {
				del = append(del, k)
			}
		}
		if len(del) > 0 {
			utils.Red.Del(ctx, del...)
		}
		fmt.Println("verify cleaned; users:", len(ids), "groups:", len(gs))
		return
	}

	sfx := strconv.FormatInt(time.Now().Unix(), 36) + "x" + strconv.Itoa(os.Getpid()%100000) // 每进程唯一，重跑不复用账号
	A := reg("vpA" + sfx)
	B := reg("vpB" + sfx)
	C := reg("vpC" + sfx)
	D := reg("vpD" + sfx)
	E := reg("vpE" + sfx)
	F := reg("vpF" + sfx)

	smallG := mkGroup(A, "vp群S"+sfx, B)           // 2 成员：常规档
	bigG := mkGroup(A, "vp群B"+sfx, B, C, D, E, F) // 6 成员：服务端阈值≥5 时走大群档
	pc := privConvOf(A.id, B.id)
	gkBig, gkSmall := "g_"+strconv.FormatInt(bigG, 10), "g_"+strconv.FormatInt(smallG, 10)

	ac, _ := dial(A, "devA")
	if ac == nil {
		fmt.Println("服务端/鉴权不可用：确认 AUTH 链路与 -host")
		os.Exit(2)
	}
	defer ac.Close()
	hbStop := make(chan struct{})
	defer close(hbStop)
	hbKeep(ac, A.id, hbStop)

	// ---- S1 私聊：3 连发 → 3 原文 + 1 聚合未读事件(Unread=3) → markRead 清、read_time 回填、A 收 Type5 ----
	bc, _ := dial(B, "devB")
	defer bc.Close()
	for i := 0; i < 3; i++ {
		sendWS(ac, A, B.id, 1, fmt.Sprintf("s1-%d", i), "vp1"+sfx+strconv.Itoa(i))
		time.Sleep(40 * time.Millisecond)
	}
	f1 := collect(bc, 4*time.Second)
	check("S1a 私聊3原文到齐", cntRaw(f1, "s1-") == 3, f1)
	agg3 := cntType(f1, 4, func(f map[string]interface{}) bool {
		return f["ConvId"] == pc && gi(f["Unread"]) == 3
	}) == 1
	check("S1b 未读按会话聚合成 1 帧 Unread=3", agg3, f1)
	check("S1c unreadList 私聊=3", unreadOf(B, pc) == 3)
	postForm("/user/markRead", url.Values{"convId": {pc}}, B.token)
	check("S1d markRead 后未读清零", unreadOf(B, pc) <= 0)
	var rt []models.Message
	utils.DB.Where("conv_key = ? AND user_id = ? AND read_time > 0", pc, A.id).Find(&rt)
	check("S1e 对端消息 read_time 已回填", len(rt) == 3, len(rt))
	f1b := collect(bc, 500*time.Millisecond) // bc 已超时弃连则跳过（Type5 发给 A）
	aa, _ := dial(A, "devA2")                // 新设备收 A 侧事件：markRead(B) 触发的 Type5 回执
	defer aa.Close()
	time.Sleep(200 * time.Millisecond)
	_ = f1b

	// ---- S2 小群：raw + Type4 ----
	bc2, _ := dial(B, "devB2") // bc 已弃用，换新设备
	defer bc2.Close()
	sendWS(ac, A, smallG, 2, "s2-g", "vp2"+sfx)
	f2 := collect(bc2, 4*time.Second)
	check("S2 小群 raw+Type4(常规档)", cntRaw(f2, "s2-g") == 1 && cntType(f2, 4, func(f map[string]interface{}) bool {
		return f["ConvId"] == gkSmall
	}) == 1, f2)

	// ---- S3 大群 Type=7 ----
	bc3, _ := dial(B, "devB3")
	defer bc3.Close()
	sendWS(ac, A, bigG, 2, "s3-big", "vp3"+sfx)
	f3 := collect(bc3, 5*time.Second)
	t7 := cntType(f3, 7, func(f map[string]interface{}) bool { return f["ConvId"] == gkBig })
	rawBig := cntRaw(f3, "s3-big") // Type7 内嵌 Msg 也含 Content，单独判"裸原文"须排除 7
	nakedRaw := 0
	for _, f := range f3 {
		if ty, _ := f["Type"].(float64); int(ty) == 2 {
			nakedRaw++
		}
	}
	_ = rawBig
	if viper.GetInt("push.groupPushMaxMembers") >= 6 && t7 == 0 {
		skip("S3 大群 Type=7", "服务端阈值>成员数，设 PUSH_GROUPPUSHMAXMEMBERS=4 复测")
	} else {
		check("S3a 大群收 Type=7 合并帧", t7 >= 1 && nakedRaw == 0, f3)
		check("S3b 大群计算式未读", unreadOf(B, gkBig) >= 1, unreadOf(B, gkBig))
		postForm("/user/markRead", url.Values{"convId": {gkBig}}, B.token)
		check("S3c markRead 后计算式归零", unreadOf(B, gkBig) <= 0)
		var cur []models.ReadCursor
		utils.DB.Where("conv_key = ? AND user_id = ?", gkBig, B.id).Find(&cur)
		check("S3d 游标已落库", len(cur) == 1 && cur[0].LastSeq >= 1, cur)
		var gmsg []models.Message
		utils.DB.Where("conv_key = ? AND content = ?", gkBig, "s3-big").Find(&gmsg)
		check("S3e 群 read_count 窗口计数=1", len(gmsg) == 1 && gmsg[0].ReadCount == 1, gmsg)
	}

	// ---- S4 多端扇出 + 设备心跳隔离 ----
	// 旧版两处坑已修：①collect 的短读超时会被 gorilla 自动弃连，导致"静默端"在观察踢线前
	//   已被测试自身杀死；②全局 sendMu 串行化所有连接写入，多端并发下心跳被饿死→活跃端误踢。
	// 现：扇出用一次性设备；隔离用例的静默/活跃端在 hbWait 窗口内绝不设置读超时（保持存活），
	//   到期后才单次读取——静默端应已被服务端踢（真断，非 i/o 超时），活跃端应仍在线并收到探测帧。
	fan, _ := dialNoHb(A, "fan")
	bc4, _ := dial(B, "devB4")
	silent, _ := dialNoHb(A, "silent")
	act, _ := dialNoHb(A, "active")
	if fan != nil && bc4 != nil && silent != nil && act != nil {
		defer bc4.Close()
		sendWS(bc4, B, A.id, 1, "s4-fan", "vp4"+sfx)
		r1 := collect(fan, 3*time.Second) // 一次性设备，collect 弃连无害
		fan.Close()
		check("S4a 多端扇出（另一设备收到）", cntRaw(r1, "s4-fan") == 1, len(r1))

		// 活跃端持续心跳（per-conn 锁，不被其他连接饿死）；静默端全程不发心跳、不读
		hbKeep(act, A.id, globalHbStop)
		time.Sleep(hbWait) // > TIMEOUT_DELAYHEARTBEAT：静默端此时必已被服务端踢
		sendWS(bc4, B, A.id, 1, "s4-alive", "vp4b"+sfx)

		// 活跃端应收到 s4-alive；静默端不该见到 s4-alive（踢于其发出之前）且终态为真断连
		alive, aerr := probeRead(act, 8*time.Second, "s4-alive")
		silGot, serr := probeRead(silent, 4*time.Second, "s4-alive")
		check("S4b 心跳隔离：静默端被踢、活跃端存活（需 TIMEOUT_DELAYHEARTBEAT≤10）",
			alive && aerr == nil && !silGot && serr != nil && isRealDrop(serr), alive, aerr, silGot, serr)
	} else {
		skip("S4 多端隔离", "拨号失败或 maxDevices 限制")
	}

	// ---- S5 同 clientMsgId 跨窗口重发 → 冷层仅 1 行 + 首帧唯一 ----
	bc5, _ := dial(B, "devB5")
	defer bc5.Close()
	for i := 0; i < 3; i++ {
		sendWS(ac, A, B.id, 1, "s5-dup", "vp5"+sfx) // 完全相同 cmid
		time.Sleep(700 * time.Millisecond)          // 跨聚合窗口，绕开批内 seen，逼 split 对账上场
	}
	f5 := collect(bc5, 4*time.Second)
	var s5rows []models.Message
	utils.DB.Where("conv_key = ? AND client_msg_id = ?", pc, "vp5"+sfx).Find(&s5rows)
	check("S5a 重发3次冷层仅1行", len(s5rows) == 1, len(s5rows))
	check("S5b 接收端原文仅1帧(副本转重放)", cntRaw(f5, "s5-dup") == 1, f5)

	// ---- S6 重放风暴取证：20 连发同 cmid，应 0 冷层唯一冲突（split 前置拦截）----
	for i := 0; i < 20; i++ {
		sendWS(ac, A, B.id, 1, "s6", "vp6"+sfx)
	}
	time.Sleep(4 * time.Second)
	var s6 []models.Message
	utils.DB.Where("conv_key = ? AND client_msg_id = ?", pc, "vp6"+sfx).Find(&s6)
	check("S6 重放风暴×20 冷层仅1行", len(s6) == 1, len(s6))

	fmt.Println("=====================================")
	if failCnt == 0 {
		fmt.Println("ALL PASS（冲突取证请看服务端日志『冲突样本』行）")
	} else {
		fmt.Printf("%d FAILURES\n", failCnt)
		os.Exit(1)
	}
}
