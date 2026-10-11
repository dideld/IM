package models

// 落盘后置放大分发（store-and-forward 的"分发/通知/回执"段）：
//   冷层写入成功后，一条消息在此放大为——收件人投递（扇出）、未读维护（含离线推送挂点）、
//   发送者 Type=6 ACK。WS 侧不再直接投递，"先落盘再投递"的顺序闸门由 persistMessage→cold writer 保证。
//   另有生产侧的应用层重投队列：同步投 Kafka 失败不立刻报错，重投超限才回 -1 ACK。

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"webapp/utils"

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

// deliverLanded 单条落盘成功后的放大投递。调用方：消费者(inline 档) / 冷 writer(batch 档 flush 成功后)。
func deliverLanded(recs []landedRecord) {
	for i := range recs {
		rec := &recs[i]
		sender := int64(rec.parsed.UserId)
		switch rec.parsed.Type {
		case 2:
			// 群聊：成员列表走 g_members 缓存（消费者热路径每条必查，绝不能裸打 MySQL）
			members := GroupMembers(uint(rec.parsed.TargetId))
			targets := make([]int64, 0, len(members))
			for _, m := range members {
				if m != sender {
					deliverLocal(m, rec.raw)
					targets = append(targets, m)
				}
			}
			pushUnread(rec, targets)
		default:
			// 私聊：直投目标（含"发给自己"的合法会话）；给自己发不累积未读
			deliverLocal(rec.parsed.TargetId, rec.raw)
			if rec.parsed.TargetId != sender {
				pushUnread(rec, []int64{rec.parsed.TargetId})
			}
		}
		ackTo(sender, rec.clientMsgId, rec.convId, rec.seq, rec.msgId, 0, "")
	}
}

// failLanded 落库失败收口：回滚热层（A2：冷是全集，热的不可丢副本必须一并撤，
// 否则 historyMsg 看不到的消息会泄漏进热层结果）→ 清去重标记（允许该 clientMsgId 未来重新落库）
// → 死信留证 → 错误 ACK-1。
// 调用后消费者照常提交 offset——毒消息不无限占住分区，失败责任移交死信与客户端。
func failLanded(rec landedRecord, reason string) {
	// 按 member 精确删除（raw 即热层写入时的 member），再按 score 兜底一次
	ctx := context.Background()
	if err := utils.Red.ZRem(ctx, rec.convId, string(rec.raw)).Err(); err != nil {
		fmt.Println("failLanded ZRem(member) failed:", err)
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
		fmt.Println("dlq push failed:", err)
	}
	utils.Red.LTrim(ctx, "msg:dlq", -500, -1)
}

// ===== 生产侧应用层重投队列 =====

type pendingProduce struct {
	data        []byte
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

// produceOrDefer WS 侧唯一入队路径：同步 Produce 失败先落进程内重投队列（2s 一轮，至多 5 次），
// 超限才回 -1 ACK。语义（拍板）：Kafka/DB 类失败在重试耗尽前"不 ACK 也不报成功"，
// 客户端靠 pending+超时重发兜底；参数非法则是 recvProc 校验阶段的即时 -1（不进本队列）。
func produceOrDefer(senderUid int64, m *Message, data []byte) {
	if err := utils.Produce(data); err == nil {
		return
	} else {
		fmt.Println("Kafka 生产失败，进入应用层重投队列:", err)
	}
	ppMu.Lock()
	if len(ppQueue) >= 10000 { // 队列上限：服务长时间不可用时快速失败而不是无限吃内存
		ppMu.Unlock()
		ackTo(senderUid, m.ClientMsgId, convIdForAck(m), 0, 0, -1, "消息服务不可用(重投队列已满)")
		return
	}
	ppQueue = append(ppQueue, pendingProduce{
		data:        append([]byte(nil), data...),
		senderUid:   senderUid,
		clientMsgId: m.ClientMsgId,
		convId:      convIdForAck(m),
		attempts:    1,
	})
	ppMu.Unlock()
	ppOnce.Do(startProducePump)
}

func startProducePump() {
	go func() {
		t := time.NewTicker(2 * time.Second)
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
				if err := utils.Produce(p.data); err != nil {
					p.attempts++
					if p.attempts > 5 {
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
				fmt.Println("【告警】Kafka 重投超限，已回失败 ACK:", p.clientMsgId)
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
			fmt.Println("g_members cache refill failed:", e)
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
		fmt.Println("CacheGroupMember failed:", err)
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
