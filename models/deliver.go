package models

// 落地扇出与生产侧韧性（store-and-forward 的"分发/通知/回执 + 背压"段）：
//   冷层写入成功后，一批消息在 landAndDeliver 内合并为"热层+门禁终值+未读"单 Pipeline，
//   exec 成功后放大为——收件人投递（扇出）、Type=4 未读事件（带权威计数）、发送者 Type=6 ACK。
//   WS 侧不再直接投递，"先落盘再投递"的顺序闸门由 聚合器→cold writer 保证（见 consume.go/store.go）。
//   另有生产侧准入三态闸与应用层重投队列：同步投 Kafka 失败不立刻报错，重投超限才回 -1 ACK。

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"

	"webapp/utils"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"golang.org/x/sync/singleflight"
)

// ackTo 向发送者全部在线设备推 Type=6 服务端确认帧（仅服务端→客户端）。
// Status 0=已落库并完成放大分发；-1=服务端拒绝（参数非法/存储不可用/落库失败），带 Reason。
// ACK 携带 Seq/MsgId：客户端本地气泡据此对账；断线重连后原样重发同一 clientMsgId，
// 服务端幂等命中会"补发一份 ACK-0（原 Seq/MsgId）"而不重复落库——客户端不需要等重投递就能收敛状态。
func ackTo(senderUid int64, clientMsgId, convId string, seq, msgId int64, status int, reason string) {
	if senderUid <= 0 {
		return
	}
	evt := map[string]interface{}{
		"Type":        6,
		"ClientMsgId": clientMsgId,
		"ConvId":      convId,
		"Seq":         seq,
		"MsgId":       msgId,
		"Status":      status,
	}
	if reason != "" {
		evt["Reason"] = reason
	}
	data, err := json.Marshal(evt)
	if err != nil {
		return
	}
	deliverLocal(senderUid, data)
}

// landAndDeliver 冷层成功后的"落地扇出"合一管道（本批一次 Redis 往返）：
// 热层 ZAdd(全员)+按会话修剪/续期 + 门禁终值 HSet + 未读 HIncrBy —— 全部合并；
// exec 成功后才做纯内存扇出：消息原文 → Type=4 未读事件（携带管道算出的权威计数）→ 发送者 Type=6 ACK(0)。
// 返回 err（内部已重试 3 次）→ 调用方对整批 failLanded 收口（其中 ZRem 清掉可能的半写热层残留）。
func landAndDeliver(recs []landedRecord) error {
	ctx := context.Background()
	hotKeep := int64(cfgInt("storage.hotKeep", 200))
	ttl := time.Duration(cfgInt("storage.hotTTLDays", 7)) * 24 * time.Hour

	// 群成员表按会话取一次（g_members 缓存，singleflight 保护）；据此规划每个 rec 的收件人
	membersOf := make(map[string][]int64, 4)
	targets := make([][]int64, len(recs))
	for i := range recs {
		rec := &recs[i]
		sender := int64(rec.parsed.UserId)
		if rec.parsed.Type == 2 {
			m := membersOf[rec.convId]
			if m == nil {
				m = GroupMembers(convGid(rec.convId))
				if m == nil {
					m = []int64{}
				}
				membersOf[rec.convId] = m
			}
			for _, uid := range m {
				if uid != sender {
					targets[i] = append(targets[i], uid)
				}
			}
		} else if rec.parsed.TargetId != sender {
			targets[i] = append(targets[i], rec.parsed.TargetId)
		}
	}

	type incrReq struct {
		uid   int64
		recIx int
	}
	var incrs []*redis.IntCmd
	var metas []incrReq
	var execErr error
	// 重试必须重建 pipeline（已 Exec 的 Cmd 不可复用），封成闭包
	run := func() error {
		incrs = incrs[:0]
		metas = metas[:0]
		pipe := utils.Red.Pipeline()
		for i := range recs {
			pipe.ZAdd(ctx, recs[i].convId, redis.Z{Score: float64(recs[i].seq), Member: recs[i].raw})
		}
		for _, cid := range distinctConvs(recs) {
			pipe.ZRemRangeByRank(ctx, cid, 0, -(hotKeep + 1))
			pipe.Expire(ctx, cid, ttl)
			pipe.Expire(ctx, "cm:"+cid, ttl)
		}
		for i := range recs {
			if recs[i].clientMsgId != "" {
				pipe.HSet(ctx, "cm:"+recs[i].convId, recs[i].clientMsgId,
					strconv.FormatInt(recs[i].seq, 10)+":"+strconv.FormatInt(recs[i].msgId, 10))
			}
		}
		for i := range recs {
			for _, uid := range targets[i] {
				incrs = append(incrs, pipe.HIncrBy(ctx, unreadKeyPrefix+strconv.FormatInt(uid, 10), recs[i].convId, 1))
				metas = append(metas, incrReq{uid: uid, recIx: i})
			}
		}
		_, err := pipe.Exec(ctx)
		return err
	}
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
		}
		if execErr = run(); execErr == nil {
			break
		}
	}
	if execErr != nil {
		return execErr
	}

	// —— exec 成功后才扇出（内存路径，无失败回滚需求）——
	for i := range recs {
		rec := &recs[i]
		for _, uid := range targets[i] {
			deliverLocal(uid, rec.raw)
		}
	}
	previewOf := make([]string, len(recs))
	for i := range recs {
		previewOf[i] = mediaPreview(recs[i].parsed)
	}
	for k, meta := range metas {
		n, err := incrs[k].Result()
		if err != nil {
			utils.Error(ctx, "unread HIncrBy 结果异常", "uid", meta.uid, "error", err)
			continue
		}
		rec := &recs[meta.recIx]
		evt, err := json.Marshal(map[string]interface{}{"Type": 4, "ConvId": rec.convId, "Seq": rec.seq, "Unread": n})
		if err == nil {
			deliverLocal(meta.uid, evt)
		}
		if pushEnabled() && !WsHub.HasOnline(meta.uid) {
			utils.OfflinePush(meta.uid, utils.PushSummary{
				ConvId: rec.convId, Seq: rec.seq, MsgId: rec.msgId,
				Sender: int64(rec.parsed.UserId), MsgType: rec.parsed.Type, Preview: previewOf[meta.recIx],
			})
		}
	}
	for i := range recs {
		rec := &recs[i]
		ackTo(int64(rec.parsed.UserId), rec.clientMsgId, rec.convId, rec.seq, rec.msgId, 0, "")
	}
	// 门禁 HASH 近窗修剪按会话一次
	for _, cid := range distinctConvs(recs) {
		trimCmSet(cid, hotKeep)
	}
	return nil
}

// convGid "g_{群id}" → 群 id（解析失败返回 0）
func convGid(convId string) uint {
	gid, err := strconv.ParseUint(strings.TrimPrefix(convId, "g_"), 10, 32)
	if err != nil {
		return 0
	}
	return uint(gid)
}

// failLanded 落库失败收口：回滚热层（A2：冷是全集，热的不可丢副本必须一并撤，
// 否则 historyMsg 看不到的消息会泄漏进热层结果）→ 清去重标记（允许该 clientMsgId 未来重新落库）
// → 死信留证 → 错误 ACK-1。
// 调用后消费者照常提交 offset——毒消息不无限占住分区，失败责任移交死信与客户端。
func failLanded(rec landedRecord, reason string) {
	// 按 member 精确删除（raw 即热层写入时的 member），再按 score 兜底一次
	ctx := context.Background()
	if err := utils.Red.ZRem(ctx, rec.convId, string(rec.raw)).Err(); err != nil {
		utils.Error(ctx, "failLanded ZRem(member) failed", "error", err)
	}
	utils.Red.ZRemRangeByScore(ctx, rec.convId,
		strconv.FormatFloat(float64(rec.seq), 'f', -1, 64),
		strconv.FormatFloat(float64(rec.seq), 'f', -1, 64))
	if rec.clientMsgId != "" {
		utils.Red.HDel(ctx, "cm:"+rec.convId, rec.clientMsgId)
	}
	dlqHold(rec, reason)
	ackTo(int64(rec.parsed.UserId), rec.clientMsgId, rec.convId, rec.seq, rec.msgId, -1, reason)
}

// dlqHold 死信列表（msg:dlq，封顶 500 条）：落库失败原文留证，供事后人工补偿/排障。
func dlqHold(rec landedRecord, reason string) {
	item, err := json.Marshal(map[string]interface{}{
		"convId": rec.convId, "seq": rec.seq, "msgId": rec.msgId,
		"sender": rec.parsed.UserId, "clientMsgId": rec.clientMsgId,
		"reason": reason, "raw": string(rec.raw), "at": time.Now().UnixMilli(),
	})
	if err != nil {
		return
	}
	ctx := context.Background()
	if err := utils.Red.RPush(ctx, "msg:dlq", item).Err(); err != nil {
		utils.Error(ctx, "dlq push failed", "error", err)
	}
	utils.Red.LTrim(ctx, "msg:dlq", -500, -1)
}

// ===== 生产侧应用层重投队列 & 入口准入三态闸 =====

type pendingProduce struct {
	data        []byte
	key         string // Kafka 消息 Key=convId：同会话恒同分区（seq 单写者前提，多分区扩展的硬依赖）
	senderUid   int64
	clientMsgId string
	convId      string
	attempts    int
}

var (
	ppMu    sync.Mutex
	ppQueue []pendingProduce
	ppOnce  sync.Once
)

const (
	ppMaxQueue     = 10000 // 重投队列上限：服务长时间不可用时快速失败而不是无限吃内存
	admissionGreen = 0.60  // 水位低于此：放行
	admissionRed   = 0.85  // 水位高于此：拒新（-1 server_busy）；之间为黄区线性节流
)

// pipelinePressure 入口管道水位 = max(冷 unit 队列, 聚合入口 consumeCh, 重投队列)。
// 这是背压信号源：停摆时消费者被 enqueueCold 阻塞、新消息在 coldCh 与 ppQueue 两头堆积，
// 水位先到红区——拒绝的新消息唯一副本仍在客户端，零丢失零重复。
func pipelinePressure() float64 {
	p := 0.0
	if ul, uc := ColdQueueLen(); uc > 0 && float64(ul)/float64(uc) > p {
		p = float64(ul) / float64(uc)
	}
	if cl, cc := consumeQueueLen(); cc > 0 && float64(cl)/float64(cc) > p {
		p = float64(cl) / float64(cc)
	}
	ppMu.Lock()
	q := float64(len(ppQueue)) / float64(ppMaxQueue)
	ppMu.Unlock()
	if q > p {
		p = q
	}
	return p
}

// produceOrDefer WS 侧唯一入队路径。三段语义：
//  1. 红区（≥0.85）直接即时 -1 ACK(Reason=server_busy)，不产生 Kafka/队列压力；
//     前端对该 reason 走 5s/10s/20s 退避重发——切断"ACK 超时→立即重发"的过载正反馈放大器。
//  2. 黄区（0.60~0.85）produce 前线性节流（0→10ms），只加延迟不丢消息。
//  3. 绿区正常同步 Produce（Key=convId）；失败进进程内重投队列，超限才回 -1 ACK。
//
// Kafka/DB 类失败在重试耗尽前"不 ACK 也不报成功"，客户端 pending 重发兜底；参数非法在 recvProc 即时 -1（不进本队列）。
func produceOrDefer(senderUid int64, m *Message, data []byte) {
	conv := convIdForAck(m)
	if p := pipelinePressure(); p >= admissionRed {
		ackTo(senderUid, m.ClientMsgId, conv, 0, 0, -1, "server_busy(系统过载，稍后自动重试)")
		return
	} else if p >= admissionGreen {
		ms := int64(10 * (p - admissionGreen) / (admissionRed - admissionGreen))
		if ms > 0 {
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}
	if err := utils.Produce(data, conv); err == nil {
		return
	} else {
		utils.Error(context.Background(), "Kafka 生产失败，进入应用层重投队列", "error", err)
	}
	ppMu.Lock()
	if len(ppQueue) >= ppMaxQueue {
		ppMu.Unlock()
		ackTo(senderUid, m.ClientMsgId, conv, 0, 0, -1, "消息服务不可用(重投队列已满)")
		return
	}
	ppQueue = append(ppQueue, pendingProduce{
		data:        append([]byte(nil), data...),
		key:         conv,
		senderUid:   senderUid,
		clientMsgId: m.ClientMsgId,
		convId:      conv,
		attempts:    1,
	})
	ppMu.Unlock()
	ppOnce.Do(startProducePump)
}

func startProducePump() {
	go func() {
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for range t.C {
			ppMu.Lock()
			if len(ppQueue) == 0 {
				ppMu.Unlock()
				continue
			}
			keep := ppQueue[:0]
			var dead []pendingProduce
			for _, p := range ppQueue {
				if err := utils.Produce(p.data, p.key); err != nil {
					p.attempts++
					if p.attempts > 3 {
						dead = append(dead, p)
					} else {
						keep = append(keep, p)
					}
				}
			}
			ppQueue = keep
			ppMu.Unlock()
			for _, p := range dead {
				ackTo(p.senderUid, p.clientMsgId, p.convId, 0, 0, -1, "消息服务不可用(重投超限)")
				utils.Warn(context.Background(), "Kafka 重投超限，已回失败 ACK", "clientMsgId", p.clientMsgId)
			}
		}
	}()
}

// ===== 群成员缓存（g_members:{gid}）=====

// gmSf 防击穿：Redis 重启/过期后的 miss 风暴里，每个群同一时刻只有一个请求回源 DB。
var gmSf singleflight.Group

const (
	gmTTL        = 15 * time.Minute // 正缓存：短 TTL 换成员变更的最终一致窗口（分发路径高频，DB 回源成本可忽略）
	gmNegTTL     = 30 * time.Second // 负缓存哨兵（哨兵成员 "0"，uid 恒 >0 不冲突）
	gmNegMember  = "0"
	gmKeyPrefixK = "g_members:"
)

// GroupMembers 群成员列表：Redis SET 优先，miss 经 singleflight 回落 DB 并回填。
// JoinGroup/CacheGroupMember 即时 SAdd 补员；成员移除路径（未来的退群/解散）应调 InvalidateGroup，
// 未显式失效时靠 15min TTL 收敛（较旧 24h 大幅收窄不一致窗口）。
func GroupMembers(gid uint) []int64 {
	ctx := context.Background()
	key := gmKeyPrefixK + strconv.FormatUint(uint64(gid), 10)
	ids, err := utils.Red.SMembers(ctx, key).Result()
	if err == nil {
		if len(ids) == 1 && ids[0] == gmNegMember {
			return nil // 负缓存命中：空群，不回源
		}
		out := parseMemberIds(ids)
		if len(out) > 0 {
			return out
		}
	}
	// miss / Redis 故障 → singleflight 回源（故障时并发请求各回各的源也无妨，Do 内部合并正常路径）
	v, _, _ := gmSf.Do(key, func() (interface{}, error) {
		members := SearchUserByGroupId(gid)
		pipe := utils.Red.Pipeline()
		if len(members) == 0 {
			pipe.SAdd(ctx, key, gmNegMember)
			pipe.Expire(ctx, key, gmNegTTL)
		} else {
			vals := make([]interface{}, 0, len(members))
			for _, m := range members {
				vals = append(vals, int64(m))
			}
			pipe.SAdd(ctx, key, vals...)
			pipe.Expire(ctx, key, gmTTL)
		}
		if _, e := pipe.Exec(ctx); e != nil {
			utils.Error(ctx, "g_members cache refill failed", "error", e)
		}
		out := make([]int64, 0, len(members))
		for _, m := range members {
			out = append(out, int64(m))
		}
		return out, nil
	})
	out, _ := v.([]int64)
	return out
}

func parseMemberIds(ids []string) []int64 {
	out := make([]int64, 0, len(ids))
	for _, s := range ids {
		if s == gmNegMember {
			continue
		}
		if n, e := strconv.ParseInt(s, 10, 64); e == nil {
			out = append(out, n)
		}
	}
	return out
}

// CacheGroupMember 加群成功时的即时缓存写入点（key 可能已过期/为负缓存，SAdd 后统一带上 TTL，
// 负缓存哨兵一并清掉防止"刚加群却被空集挡 30s"）。
func CacheGroupMember(gid, uid uint) {
	if gid == 0 || uid == 0 {
		return
	}
	ctx := context.Background()
	key := gmKeyPrefixK + strconv.FormatUint(uint64(gid), 10)
	pipe := utils.Red.Pipeline()
	pipe.SRem(ctx, key, gmNegMember)
	pipe.SAdd(ctx, key, int64(uid))
	pipe.Expire(ctx, key, gmTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		utils.Error(ctx, "CacheGroupMember failed", "error", err)
	}
}

// InvalidateGroup 成员发生变更（退群/解散）时的显式失效点——当前项目尚无退群路径，
// 新增相关功能时**必须**调用本函数，否则最长 15min 内仍会向旧成员表扇出。
func InvalidateGroup(gid uint) {
	if gid == 0 {
		return
	}
	utils.Red.Del(context.Background(), gmKeyPrefixK+strconv.FormatUint(uint64(gid), 10))
}

// getReadCursor 已读水位：Redis read: 键优先，缺失回落 DB 账本（user_read_cursor）。
func getReadCursor(userId int64, convId string) int64 {
	ctx := context.Background()
	key := readKeyPrefix + strconv.FormatInt(userId, 10) + ":" + convId
	if v, err := utils.Red.Get(ctx, key).Result(); err == nil {
		if n, e := strconv.ParseInt(v, 10, 64); e == nil {
			return n
		}
	}
	var rc ReadCursor
	if err := utils.DB.Where("user_id = ? AND conv_key = ?", userId, convId).First(&rc).Error; err == nil {
		return int64(rc.LastSeq)
	}
	return 0
}

// mediaPreview 离线推送摘要（正文截断 30 字符；媒体类型给占位符）。
func mediaPreview(m Message) string {
	switch m.Media {
	case 3:
		return "[语音]"
	case 4, 2:
		return "[图片]"
	}
	r := []rune(m.Content)
	if len(r) > 30 {
		return string(r[:30]) + "…"
	}
	return m.Content
}

// pushEnabled 探测"是否值得做在线探测/推送挂点"——默认关时整条离线判定路径零开销。
func pushEnabled() bool {
	return viper.GetBool("push.enabled") || viper.GetBool("push.debug")
}
