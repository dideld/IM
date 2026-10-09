package models

// 消息冷热存储与会话状态层（先落盘、后投递——store-and-forward）：
//   会话 convId：私聊 "p_{小id}_{大id}"、群聊 "g_{群id}"（convIdOf 是唯一来源），
//   热层/冷层/未读数/事件推送共用，杜绝用户id与群id串号。
//   热层 = Redis ZSet（score = 会话级递增 seq，单会话修剪 + 滑动 TTL）
//   冷层 = MySQL message 表（主键 id = 雪花 msgId，(conv_key, seq) 唯一，游标分页）
//   未读 = Redis HASH unread:{uid}（field=convId）
//   去重 = Redis HASH cm:{convId}（field=clientMsgId，value="P"→"seq:msgId"；HSetNX 做幂等门禁，
//          重放命中回补 ACK 而不重复落库；失败路径 HDEL 允许重新落库）
//   死信 = Redis LIST msg:dlq（落库失败原文留证，封顶 500）
//   群成员缓存 = Redis SET g_members:{gid}（消费者热路径每条必查，见 deliver.go）
//   投递与 ACK 一律在冷层写入成功后由 deliverLanded 放大（batch 攒批 / inline 单条立即写，
//   storage.coldMode 只是节奏差异，"冷层成功才投递"的闸门两种模式相同）。

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"webapp/utils"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// convIdOf 会话 id 生成的唯一来源（前端 foot.html 有镜像实现，改这里必须同步改）
func convIdOf(msgType int, userId int64, targetId int64) string {
	if msgType == 2 {
		return "g_" + strconv.FormatInt(targetId, 10)
	}
	return convIdPrivate(userId, targetId)
}

// convIdPrivate 私聊双向归一化 key
func convIdPrivate(a, b int64) string {
	x, y := a, b
	if x > y {
		x, y = y, x
	}
	return "p_" + strconv.FormatInt(x, 10) + "_" + strconv.FormatInt(y, 10)
}

// ConvIdPrivate 供读接口做 userIdA/userIdB → convId 的兼容推导
func ConvIdPrivate(a, b int64) string { return convIdPrivate(a, b) }

const (
	seqKeyPrefix    = "seq:"
	unreadKeyPrefix = "unread:"
	readKeyPrefix   = "read:"
)

// landedRecord 落盘管线的一条消息：raw=客户端原始字节（热层 member 与投递共用，避免二次序列化损耗），
// parsed=结构视图（字段落冷库、分发寻址用），done=batch 模式下冷 writer 回写落库结果。
type landedRecord struct {
	raw         []byte
	parsed      Message
	convId      string
	seq         int64
	msgId       int64
	clientMsgId string
	done        chan error
}

// toColdRow 还原冷存储行（主键显式写雪花 msgId）。
func (rec *landedRecord) toColdRow() Message {
	m := rec.parsed
	m.Model = gorm.Model{ID: uint(rec.msgId)}
	m.Seq = rec.seq
	m.ConvKey = rec.convId
	return m
}

var coldCh = make(chan landedRecord, 8192)

// coldStop 关闭后冷存 worker 排空队列、落完最后一批再退出（优雅停机）
var coldStop chan struct{}

// coldDone 由 worker 在完成最终 flush 退出前 close，供停机方同步等待
var coldDone chan struct{}

func cfgInt(key string, def int) int {
	if v := viper.GetInt(key); v > 0 {
		return v
	}
	return def
}

// coldModeVal batch=攒批落库（默认，高吞吐）；inline=单条立即写（低时延）。
// 两种模式都要求"冷层写入成功"才投递+ACK——不存在"只落热层就投递"的弱可靠档。
func coldModeVal() string {
	if viper.GetString("storage.coldMode") == "inline" {
		return "inline"
	}
	return "batch"
}

// persistMessage Kafka 消费回调（at-least-once：返回 nil 消费端才提交 offset）。
// 管线：幂等门禁 → 发 seq → 热层 → 冷写（成功为闸门）→ deliverLanded（分发+未读+ACK）。
// 失败分级（拍板语义）：
//   - Redis 瞬时故障 → 返回 error 不提交，Kafka 重投；
//   - 冷写重试耗尽 / 存储持续不可用 → failLanded（清标记+死信+ACK-1）后照常提交，毒消息不占分区。
func persistMessage(msgData []byte) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("persistMessage panic (按失败提交，原文已尽力死信):", r)
		}
	}()
	jsonMsg := Message{}
	// 非 JSON（系统提示语等误入链路）直接跳过，防止污染会话 key（历史上 msg_0_0 就是这么来的）
	if err := json.Unmarshal(msgData, &jsonMsg); err != nil {
		fmt.Println("persistMessage skip invalid payload:", err)
		return nil
	}
	if jsonMsg.Type != 1 && jsonMsg.Type != 2 {
		return nil // 心跳等不该进持久化链路
	}

	convId := convIdOf(jsonMsg.Type, int64(jsonMsg.UserId), jsonMsg.TargetId)
	msgId := utils.GenSnowflakeId()
	ctx := context.Background()

	if viper.GetBool("storage.skipBenchPersist") && strings.HasPrefix(jsonMsg.ClientMsgId, "bench-") {
		return nil // 压测旁路：不落库也不 ACK——要测"落库→ACK"全链路档必须关闭本开关
	}

	rec := landedRecord{
		raw:         msgData,
		parsed:      jsonMsg,
		convId:      convId,
		msgId:       msgId,
		clientMsgId: jsonMsg.ClientMsgId,
	}

	// ---- 幂等门禁：HSetNX "P" 占位；已有终值=重放，回补原 ACK 不重复落库 ----
	if rec.clientMsgId != "" {
		ck := "cm:" + convId
		created, err := utils.Red.HSetNX(ctx, ck, rec.clientMsgId, "P").Result()
		if err != nil {
			return err // Redis 瞬时故障：不提交，Kafka 重投
		}
		if !created {
			v, _ := utils.Red.HGet(ctx, ck, rec.clientMsgId).Result()
			if v != "" && v != "P" {
				if parts := strings.SplitN(v, ":", 2); len(parts) == 2 {
					sq, _ := strconv.ParseInt(parts[0], 10, 64)
					mi, _ := strconv.ParseInt(parts[1], 10, 64)
					ackTo(int64(jsonMsg.UserId), rec.clientMsgId, convId, sq, mi, 0, "") // 重放只补回执
				}
				return nil
			}
			// 残留 "P"：单消费者串行处理，说明上次尝试死在中途（offset 未提交才重投到此），本次继续落库
		}
	}

	// ---- 发 seq（会话级严格递增；瞬时故障走 Kafka 重投）----
	seq, err := redIncrRetry(ctx, seqKeyPrefix+convId)
	if err != nil {
		fmt.Println("seq INCR failed(重投):", convId, err)
		return err
	}
	rec.seq = seq

	// ---- 热层 + 去重标记终值化：一次 Pipeline ----
	hotKeep := int64(cfgInt("storage.hotKeep", 200))
	ttl := time.Duration(cfgInt("storage.hotTTLDays", 7)) * 24 * time.Hour
	pipe := utils.Red.Pipeline()
	pipe.ZAdd(ctx, convId, redis.Z{Score: float64(seq), Member: msgData})
	pipe.ZRemRangeByRank(ctx, convId, 0, -(hotKeep + 1))
	pipe.Expire(ctx, convId, ttl)
	if rec.clientMsgId != "" {
		ck := "cm:" + convId
		pipe.HSet(ctx, ck, rec.clientMsgId, strconv.FormatInt(seq, 10)+":"+strconv.FormatInt(msgId, 10))
		pipe.Expire(ctx, ck, ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		fmt.Println("Redis hot persist Error(重投):", err)
		return err
	}
	if rec.clientMsgId != "" {
		trimCmSet(convId, hotKeep)
	}

	// ---- 冷写闸门：成功才允许投递 ----
	if coldModeVal() == "inline" {
		if err := coldInsertRetry([]landedRecord{rec}); err != nil {
			failLanded(rec, "冷存储写入失败")
			return nil
		}
		deliverLanded([]landedRecord{rec})
		return nil
	}
	rec.done = make(chan error, 1)
	coldCh <- rec // 队列满则反压消费者（宁慢勿丢）
	if err := <-rec.done; err != nil {
		return nil // 冷 writer 已完成 failLanded（清标记/死信/错误 ACK）
	}
	return nil
}

// redIncrRetry seq 发号带小重试：区分"瞬时抖动"和"持续不可用"，前者靠 Kafka 重投自愈。
func redIncrRetry(ctx context.Context, key string) (int64, error) {
	var (
		seq int64
		err error
	)
	for i := 0; i < 3; i++ {
		if i > 0 {
			time.Sleep(time.Duration(50*(i+1)) * time.Millisecond)
		}
		if seq, err = utils.Red.Incr(ctx, key).Result(); err == nil {
			return seq, nil
		}
	}
	return 0, err
}

// trimCmSet 去重 HASH 近窗修剪：超 2×hotKeep 随机淘汰（Redis<6.2 无 HRandField 时放弃修剪，靠 key TTL 兜底）。
func trimCmSet(convId string, hotKeep int64) {
	ctx := context.Background()
	ck := "cm:" + convId
	n, err := utils.Red.HLen(ctx, ck).Result()
	if err != nil || n <= 2*hotKeep {
		return
	}
	fields, err := utils.Red.HRandField(ctx, ck, int(n-2*hotKeep)).Result()
	if err != nil || len(fields) == 0 {
		return
	}
	utils.Red.HDel(ctx, ck, fields...)
}

// pushUnread 未读 +1 并向在线端推 Type=4 事件；开启推送挂点时对"无任何在线设备"的收件人调 OfflinePush。
// 注意：离线判定走 Hub probe 通道（clients 只能 Run 内触碰）；push 开关关闭时零探测开销。
func pushUnread(rec *landedRecord, uids []int64) {
	ctx := context.Background()
	preview := mediaPreview(rec.parsed)
	for _, uid := range uids {
		uk := unreadKeyPrefix + strconv.FormatInt(uid, 10)
		n, err := utils.Red.HIncrBy(ctx, uk, rec.convId, 1).Result()
		if err != nil {
			fmt.Println("unread HIncrBy error:", uk, err)
			continue
		}
		evt, err := json.Marshal(map[string]interface{}{"Type": 4, "ConvId": rec.convId, "Seq": rec.seq, "Unread": n})
		if err != nil {
			continue
		}
		// 仅内存推送，不落任何存储；离线用户下次 unreadList 拉全量补偿
		deliverLocal(uid, evt)
		if pushEnabled() && !WsHub.HasOnline(uid) {
			utils.OfflinePush(uid, utils.PushSummary{
				ConvId: rec.convId, Seq: rec.seq, MsgId: rec.msgId,
				Sender: int64(rec.parsed.UserId), MsgType: rec.parsed.Type, Preview: preview,
			})
		}
	}
}

// ConvHasMember 会话成员校验（HTTP 鉴权后仍要挡"读别人会话"）：
// p_{a}_{b} 需为两端之一；g_{群id} 需在该群 contact 成员表内。
func ConvHasMember(convId string, uid int64) bool {
	if uid <= 0 {
		return false
	}
	if strings.HasPrefix(convId, "g_") {
		gid, err := strconv.ParseUint(convId[2:], 10, 32)
		if err != nil {
			return false
		}
		for _, m := range SearchUserByGroupId(uint(gid)) {
			if int64(m) == uid {
				return true
			}
		}
		return false
	}
	if strings.HasPrefix(convId, "p_") {
		parts := strings.Split(convId[2:], "_")
		if len(parts) != 2 {
			return false
		}
		a, e1 := strconv.ParseInt(parts[0], 10, 64)
		b, e2 := strconv.ParseInt(parts[1], 10, 64)
		if e1 != nil || e2 != nil {
			return false
		}
		return a == uid || b == uid
	}
	return false
}

// ReadCursor 会话已读游标落库表（Redis read: 键的持久账本；群聊逐条回执的水位依据）
type ReadCursor struct {
	gorm.Model
	UserId  uint   `gorm:"uniqueIndex:uk_user_conv"`
	ConvKey string `gorm:"uniqueIndex:uk_user_conv;size:64"`
	LastSeq int64
}

func (table *ReadCursor) TableName() string {
	return "user_read_cursor"
}

// convPeer 私聊 key "p_a_b" 取对端（非私聊或不含该用户返回 0）
func convPeer(convId string, uid int64) int64 {
	if !strings.HasPrefix(convId, "p_") {
		return 0
	}
	parts := strings.Split(convId[2:], "_")
	if len(parts) != 2 {
		return 0
	}
	a, e1 := strconv.ParseInt(parts[0], 10, 64)
	b, e2 := strconv.ParseInt(parts[1], 10, 64)
	if e1 != nil || e2 != nil {
		return 0
	}
	if a == uid {
		return b
	}
	if b == uid {
		return a
	}
	return 0
}

// MarkRead 打开会话即已读：
//  1. Redis 游标 + 清未读（游标先取旧值，群聊逐条计数要用窗口 (old, max]）；
//  2. 游标 upsert 落库；
//  3. 私聊：回填对端消息 read_time + 向对端推 Type=5；
//  4. 群聊（逐条回执已落地）：新已读窗口内他人消息 read_count+1（水位单调天然防重计），
//     并向除已读者外的全体在线成员推 Type=5 {ReadSeq, Reader}，前端按 Reader 去重聚成"N 人已读"。
//
// markRead 频率低（开会话/收到新消息时），同步写库可接受；若上量改走独立批量 channel。
func MarkRead(userId int64, convId string) {
	if convId == "" {
		return
	}
	ctx := context.Background()
	max, _ := utils.Red.Get(ctx, seqKeyPrefix+convId).Result()
	old := getReadCursor(userId, convId) // 必须在覆盖游标前取旧水位

	pipe := utils.Red.Pipeline()
	pipe.HDel(ctx, unreadKeyPrefix+strconv.FormatInt(userId, 10), convId)
	if max != "" {
		pipe.Set(ctx, readKeyPrefix+strconv.FormatInt(userId, 10)+":"+convId, max, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		fmt.Println("MarkRead error:", err)
	}
	if max == "" {
		return // 该会话还没有消息，无游标可存
	}
	seq, _ := strconv.ParseInt(max, 10, 64)

	// 游标落库（复合唯一 user_id+conv_key，冲突则前移）
	err := utils.DB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}, {Name: "conv_key"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"last_seq": seq, "updated_at": time.Now()}),
	}).Create(&ReadCursor{UserId: uint(userId), ConvKey: convId, LastSeq: seq}).Error
	if err != nil {
		fmt.Println("read cursor upsert failed:", err)
	}

	if strings.HasPrefix(convId, "p_") {
		// 私聊：把游标之前、对方发给我的消息回填 read_time
		res := utils.DB.Model(&Message{}).
			Where("conv_key = ? AND seq > 0 AND seq <= ? AND user_id <> ? AND read_time = 0", convId, seq, userId).
			Update("read_time", time.Now().UnixMilli())
		if res.Error != nil {
			fmt.Println("read_time backfill failed:", res.Error)
		}
		// 对端在线则推已读回执事件（多端扇出自动覆盖其所有设备）
		if peer := convPeer(convId, userId); peer > 0 {
			evt, err := json.Marshal(map[string]interface{}{"Type": 5, "ConvId": convId, "ReadSeq": seq, "Reader": userId})
			if err == nil {
				deliverLocal(peer, evt)
			}
		}
		return
	}

	// 群聊逐条已读回执
	if strings.HasPrefix(convId, "g_") && seq > old {
		gid, e := strconv.ParseUint(convId[2:], 10, 32)
		if e != nil {
			return
		}
		res := utils.DB.Model(&Message{}).
			Where("conv_key = ? AND seq > ? AND seq <= ? AND user_id <> ?", convId, old, seq, userId).
			UpdateColumn("read_count", gorm.Expr("read_count + 1"))
		if res.Error != nil {
			fmt.Println("group read_count window update failed:", res.Error)
		}
		evt, err := json.Marshal(map[string]interface{}{"Type": 5, "ConvId": convId, "ReadSeq": seq, "Reader": userId})
		if err != nil {
			return
		}
		for _, m := range GroupMembers(uint(gid)) {
			if m != userId {
				deliverLocal(m, evt)
			}
		}
	}
}

// PeerReadSeq 会话对端的已读游标（私聊气泡"已读"状态用）：Redis 优先，缺则回落 DB。
// 注意：仅私聊语义；群聊逐条已读走消息行 read_count + Type=5 事件，不用本接口。
func PeerReadSeq(convId string, uid int64) int64 {
	peer := convPeer(convId, uid)
	if peer <= 0 {
		return 0
	}
	return getReadCursor(peer, convId)
}

// UnreadMap 返回 {convId: 未读数}，无数据返回空 map
func UnreadMap(userId int64) map[string]int64 {
	out := map[string]int64{}
	m, err := utils.Red.HGetAll(context.Background(), unreadKeyPrefix+strconv.FormatInt(userId, 10)).Result()
	if err != nil {
		fmt.Println("UnreadMap error:", err)
		return out
	}
	for k, v := range m {
		if n, e := strconv.ParseInt(v, 10, 64); e == nil && n > 0 {
			out[k] = n
		}
	}
	return out
}

// StartColdWriter 冷存储批量落盘 worker（batch 档）：攒够 coldBatchSize 条或等 coldFlushMs 毫秒即写 MySQL。
// 顺序铁律：CreateInBatches 成功 → deliverLanded（投递+ACK）→ 回 done；失败（重试后）→ failLanded → done(err)。
// 必须在 utils.InitMysql() 之后、Kafka 消费者启动之前调用
func StartColdWriter() {
	coldStop = make(chan struct{})
	coldDone = make(chan struct{})
	batchSize := cfgInt("storage.coldBatchSize", 100)
	// 默认 50ms：先落盘再投递后，攒批等待直接叠加到消息端到端时延（原 200ms 是纯归档链路时可容忍）
	flush := time.Duration(cfgInt("storage.coldFlushMs", 50)) * time.Millisecond

	go func() {
		batch := make([]landedRecord, 0, batchSize)
		ticker := time.NewTicker(flush)
		defer ticker.Stop()

		flushBatch := func() {
			if len(batch) == 0 {
				return
			}
			if err := coldInsertRetry(batch); err != nil {
				for i := range batch {
					failLanded(batch[i], "冷存储写入失败")
					if batch[i].done != nil {
						batch[i].done <- err
					}
				}
			} else {
				deliverLanded(batch) // 冷层成功是投递的前置闸门
				for i := range batch {
					if batch[i].done != nil {
						batch[i].done <- nil
					}
				}
			}
			batch = batch[:0]
		}

		for {
			select {
			case rec := <-coldCh:
				batch = append(batch, rec)
				if len(batch) >= batchSize {
					flushBatch()
				}
			case <-ticker.C:
				flushBatch()
			case <-coldStop:
				// 排空在途队列后做最后一次落盘+投递再退出（宁慢勿丢）
				for {
					select {
					case rec := <-coldCh:
						batch = append(batch, rec)
						if len(batch) >= batchSize {
							flushBatch()
						}
					default:
						flushBatch()
						fmt.Println("cold writer stopped, final flush+deliver done")
						close(coldDone)
						return
					}
				}
			}
		}
	}()
	fmt.Println("cold writer started, mode:", coldModeVal(), "batchSize:", batchSize, "flushInterval:", flush)
}

// coldInsertRetry 冷层写入（batch/inline 共用）：3 次尝试，主键/唯一冲突跳过（DoNothing）。
func coldInsertRetry(batch []landedRecord) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
		}
		msgs := make([]Message, 0, len(batch))
		for i := range batch {
			msgs = append(msgs, batch[i].toColdRow())
		}
		r := utils.DB.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(msgs, len(msgs))
		err = r.Error
		if err == nil {
			if r.RowsAffected < int64(len(msgs)) {
				// 冲突跳过的行要大声告警：多实例 workerId 重复或重放会在这里体现
				fmt.Printf("【警告】冷存储 %d/%d 条冲突被跳过（重放或 workerId 重复）：检查 snowflake.workerId！\n",
					int64(len(msgs))-r.RowsAffected, len(msgs))
			}
			return nil
		}
	}
	fmt.Println("cold persist MySQL batch failed, size:", len(batch), "err:", err)
	return err
}

// StopColdWriter 通知冷存 worker 排空并落完最后一批（含投递/ACK 收尾），阻塞至其真正退出（最长 10s）
func StopColdWriter() {
	if coldStop == nil {
		return
	}
	coldStop <- struct{}{} // 与 worker 握手：它能收到即意味着它将开始收尾
	select {
	case <-coldDone:
	case <-time.After(10 * time.Second):
		fmt.Println("【警告】cold writer 收尾超时，可能有在途消息未落库/未投递")
	}
}

// LoadHistory 冷层游标分页读取：按 conv_key 取 seq < beforeSeq 的最近 size 条（seq 降序）。
// 返回行含 ReadCount（群逐条已读的服务端权威计数，前端冷翻页回灌时直接取用）。
func LoadHistory(convId string, beforeSeq int64, size int) []Message {
	if convId == "" || size <= 0 {
		return []Message{}
	}
	if size > 100 {
		size = 100
	}
	q := utils.DB.Where("conv_key = ?", convId)
	if beforeSeq > 0 {
		q = q.Where("seq < ?", beforeSeq)
	}
	var list []Message
	q.Order("seq desc").Limit(size).Find(&list)
	return list
}
