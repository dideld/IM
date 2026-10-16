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
	"strconv"
	"strings"
	"time"
	"webapp/utils"

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
// parsed=结构视图（字段落冷库、分发寻址用），kmsg=来源 Kafka 消息（冷 writer flush 后跨协程推进提交——
// "消费者不等同步落盘"的流水线设计：提交权随消息移交，见 StartKafkaConsumer/ErrCommitDeferred）。
type landedRecord struct {
	raw         []byte
	parsed      Message
	convId      string
	seq         int64
	msgId       int64
	clientMsgId string
	kmsg        utils.KafkaMsg
}

// toColdRow 还原冷存储行（主键显式写雪花 msgId）。
func (rec *landedRecord) toColdRow() Message {
	m := rec.parsed
	m.Model = gorm.Model{ID: uint(rec.msgId)}
	m.Seq = rec.seq
	m.ConvKey = rec.convId
	return m
}

// coldCh/consumeCh 与聚合器见 consume.go（unit 通道，满=停摆天然背压）

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
//  4. 群聊（逐条回执已落地）：新已读窗口内他人消息 read_count+1（markReadWindowTx 行锁串行防双计），
//     并向除已读者外的全体在线成员推 Type=5 {ReadSeq, Reader}，前端按 Reader 去重聚成"N 人已读"。
//
// markRead 频率低（开会话/收到新消息时），同步写库可接受；若上量改走独立批量 channel。
func MarkRead(userId int64, convId string) {
	if convId == "" {
		return
	}
	ctx := context.Background()
	max, _ := utils.Red.Get(ctx, seqKeyPrefix+convId).Result()

	pipe := utils.Red.Pipeline()
	pipe.HDel(ctx, unreadKeyPrefix+strconv.FormatInt(userId, 10), convId)
	if max != "" {
		pipe.Set(ctx, readKeyPrefix+strconv.FormatInt(userId, 10)+":"+convId, max, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		utils.Error(ctx, "MarkRead error", "error", err)
	}
	if max == "" {
		return // 该会话还没有消息，无游标可存
	}
	seq, _ := strconv.ParseInt(max, 10, 64)

	if strings.HasPrefix(convId, "p_") {
		// 游标落库（复合唯一 user_id+conv_key，冲突则前移）
		err := utils.DB.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "user_id"}, {Name: "conv_key"}},
			DoUpdates: clause.Assignments(map[string]interface{}{"last_seq": seq, "updated_at": time.Now()}),
		}).Create(&ReadCursor{UserId: uint(userId), ConvKey: convId, LastSeq: seq}).Error
		if err != nil {
			utils.Error(ctx, "read cursor upsert failed", "error", err)
		}
		// 私聊：把游标之前、对方发给我的消息回填 read_time
		res := utils.DB.Model(&Message{}).
			Where("conv_key = ? AND seq > 0 AND seq <= ? AND user_id <> ? AND read_time = 0", convId, seq, userId).
			Update("read_time", time.Now().UnixMilli())
		if res.Error != nil {
			utils.Error(ctx, "read_time backfill failed", "error", res.Error)
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

	// 群聊逐条已读回执：窗口计数在事务内串行化（A4），广播走 g_members 缓存
	if strings.HasPrefix(convId, "g_") {
		old := markReadWindowTx(userId, convId, seq)
		if seq <= old {
			return // 多端并发时水位已被推进，本次窗口为空，免打扰广播
		}
		gid, e := strconv.ParseUint(convId[2:], 10, 32)
		if e != nil {
			return
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

// markReadWindowTx 群已读窗口计数的事务化串行（A4：修复双开标签页并发 MarkRead 双计 read_count）。
// 顺序讲究：先"占位 upsert"（ON DUPLICATE 原值自赋值）保证行存在并拿住 X 行锁——
// 直接对不存在的行 SELECT FOR UPDATE 在 RR 下锁的是间隙，两个首读事务会互相死锁；
// 再 SELECT FOR UPDATE 取权威旧水位 → (old, seq] 窗口 +1 → 前移水位。
func markReadWindowTx(userId int64, convId string, seq int64) int64 {
	var old int64
	err := utils.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "user_id"}, {Name: "conv_key"}},
			DoUpdates: clause.Assignments(map[string]interface{}{"last_seq": gorm.Expr("last_seq")}),
		}).Create(&ReadCursor{UserId: uint(userId), ConvKey: convId}).Error; err != nil {
			return err
		}
		var rc ReadCursor
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ? AND conv_key = ?", userId, convId).First(&rc).Error; err != nil {
			return err
		}
		old = int64(rc.LastSeq)
		if seq <= old {
			return nil
		}
		if err := tx.Model(&Message{}).
			Where("conv_key = ? AND seq > ? AND seq <= ? AND user_id <> ?", convId, old, seq, userId).
			UpdateColumn("read_count", gorm.Expr("read_count + 1")).Error; err != nil {
			return err
		}
		return tx.Model(&ReadCursor{}).Where("id = ?", rc.ID).
			UpdateColumns(map[string]interface{}{"last_seq": seq, "updated_at": time.Now()}).Error
	})
	if err != nil {
		utils.Error(context.Background(), "markReadWindowTx failed", "error", err)
	}
	return old
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
		utils.Error(context.Background(), "UnreadMap error", "error", err)
		return out
	}
	for k, v := range m {
		if n, e := strconv.ParseInt(v, 10, 64); e == nil && n > 0 {
			out[k] = n
		}
	}
	return out
}

// StartColdWriter 冷存储 worker（唯一落库/投递/提交路径，inline 档=unit size 1 的特例）：
// 攒够 coldBatchSize 条记录或等 coldFlushMs 即 flush。
// 顺序铁律（一次 flush 内）：CreateInBatches 成功 → landAndDeliver（热层+门禁终值+未读 单 Pipeline →
// 投递/ACK 扇出）→ commitFrom(本组全部输入 offset)；任一硬失败 → failLanded 收口后**照常提交**（毒消息不占分区）。
// 必须在 utils.InitMysql() 之后、Kafka 消费者启动之前调用。
func StartColdWriter() {
	coldStop = make(chan struct{})
	coldDone = make(chan struct{})
	batchSize := cfgInt("storage.coldBatchSize", 100)
	if coldModeVal() == "inline" {
		batchSize = 1 // inline=每条立即 flush 的同一路径（窗口 0、批 1），不再维护第二套写法
	}
	// 默认 50ms：先落盘再投递后，攒批等待直接叠加到消息端到端时延
	flush := time.Duration(cfgInt("storage.coldFlushMs", 50)) * time.Millisecond

	go func() {
		units := make([]coldUnit, 0, 8)
		recCount := 0
		ticker := time.NewTicker(flush)
		defer ticker.Stop()

		flushUnits := func() {
			if len(units) == 0 {
				return
			}
			batch := units
			units = units[:0]
			recCount = 0
			// 批级 panic 保险（不可协程式 recover 后退出——writer 死了移交消息变静默丢失）
			func() {
				defer func() {
					if r := recover(); r != nil {
						utils.Error(context.Background(), "cold writer flush panic（整批失败收口并推进提交）", "recover", r)
						var all []landedRecord
						for _, u := range batch {
							all = append(all, u.recs...)
						}
						failRecs(all, "cold writer panic")
					}
				}()
				var all []landedRecord
				for _, u := range batch {
					all = append(all, u.recs...)
				}
				// 落库前幂等对账（消除门禁终值窗口竞态）：INSERT 成功~land Pipeline 写终值之间，
				// 重发副本可能穿过 HSetNX 门禁混进后续 unit——直接插入会撞 uk_conv_cmid（跳过+告警+
				// 终值/热层 score 被副本新 seq 顶掉）。对账把副本转成"重放"：回补原 seq/msgId 的 ACK，
				// 不占热层、不重计未读、不改写终值。writer 单协程顺序执行 ⇒ 查插在"本批完成前"闭环无竞态。
				fresh, replays := splitAlreadyLanded(all)
				if len(fresh) > 0 && coldInsertRetry(fresh) != nil {
					failRecs(fresh, "冷存储写入失败")
				} else if len(fresh) > 0 {
					if err := landAndDeliver(fresh); err != nil {
						utils.Error(context.Background(), "land pipeline 失败，整批失败收口", "error", err)
						failRecs(fresh, "投递落盘失败")
					}
				}
				for _, rp := range replays {
					ackTo(int64(rp.parsed.UserId), rp.clientMsgId, rp.convId, rp.seq, rp.msgId, 0, "")
				}
				// 提交恒推进（成功=已投递；失败=failLanded 死信+错误 ACK 收口）——批 FIFO ⇒ 连续前缀
				var commitTo []utils.KafkaMsg
				for _, u := range batch {
					commitTo = append(commitTo, u.commitTo...)
				}
				commitFrom(commitTo)
			}()
		}

		for {
			select {
			case u := <-coldCh:
				units = append(units, u)
				recCount += len(u.recs)
				if recCount >= batchSize {
					flushUnits()
				}
			case <-ticker.C:
				flushUnits()
			case <-coldStop:
				// 排空在途队列后做最后一轮 落库+投递+提交 再退出（宁慢勿丢）
				for {
					select {
					case u := <-coldCh:
						units = append(units, u)
						recCount += len(u.recs)
						if recCount >= batchSize {
							flushUnits()
						}
					default:
						flushUnits()
						utils.Info(context.Background(), "cold writer stopped, final flush+deliver+commit done")
						close(coldDone)
						return
					}
				}
			}
		}
	}()
	utils.Info(context.Background(), "cold writer started", "mode", coldModeVal(), "batchSize", batchSize, "flushInterval", flush.String())
}

// failRecs 对一批记录执行 failLanded 收口（清标记+死信+错误 ACK）。
func failRecs(recs []landedRecord, reason string) {
	for i := range recs {
		failLanded(recs[i], reason)
	}
}

// splitAlreadyLanded 落库前幂等对账：按 (conv_key, client_msg_id) 索引查出已落库的副本重定向为"重放"。
// 无 clientMsgId 的记录（UDP 旁路等）不参与对账。查询走 uk_conv_cmid 复合索引，单批 ≤500 组。
//
// 两层防御：①先做批内(跨 unit)去重——同一 flush 合并多个 unit 时，重复副本双双未写库、
// lookup 查无可见，直接同语句 INSERT 会撞 uk_conv_cmid（跳过+告警+seq 洞）；保留首见者
// （入队更早、seq 更小），后见静默丢弃：客户端由首见者的落库路径拿到 ACK，offset 随批提交。
// ②再对 DB 已有行做 replay 重定向（跨 flush 的重发/重放副本）。
func splitAlreadyLanded(recs []landedRecord) (fresh []landedRecord, replays []landedRecord) {
	seen := make(map[string]bool, len(recs))
	deduped := make([]landedRecord, 0, len(recs))
	for _, rec := range recs {
		if rec.clientMsgId != "" {
			key := rec.convId + "|" + rec.clientMsgId
			if seen[key] {
				continue
			}
			seen[key] = true
		}
		deduped = append(deduped, rec)
	}
	recs = deduped

	byConv := make(map[string][]string, 8)
	for i := range recs {
		if recs[i].clientMsgId != "" {
			byConv[recs[i].convId] = append(byConv[recs[i].convId], recs[i].clientMsgId)
		}
	}
	if len(byConv) == 0 {
		return recs, nil
	}
	type landedRow struct {
		ConvKey     string
		ClientMsgId string `gorm:"column:client_msg_id"`
		Seq         int64
		ID          uint64 `gorm:"column:id"`
	}
	var where strings.Builder
	var args []interface{}
	for cid, cmids := range byConv {
		if where.Len() > 0 {
			where.WriteString(" OR ")
		}
		where.WriteString("(conv_key = ? AND client_msg_id IN (?))")
		args = append(args, cid, cmids)
	}
	var rows []landedRow
	if err := utils.DB.Table("message").Select("conv_key, client_msg_id, seq, id").
		Where(where.String(), args...).Find(&rows).Error; err != nil {
		utils.Error(context.Background(), "幂等对账查询失败(按全新处理，唯一索引兜底)", "error", err)
		return recs, nil
	}
	hit := make(map[string]landedRow, len(rows))
	for _, r := range rows {
		hit[r.ConvKey+"|"+r.ClientMsgId] = r
	}
	for _, rec := range recs {
		if rec.clientMsgId != "" {
			if r, ok := hit[rec.convId+"|"+rec.clientMsgId]; ok {
				rec.seq, rec.msgId = r.Seq, int64(r.ID) // 回补原身份
				replays = append(replays, rec)
				continue
			}
		}
		fresh = append(fresh, rec)
	}
	return fresh, replays
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
				// 冲突跳过的行要大声告警：workerId 撞号（不同消息同 ID）与 Redis 丢失后的重放（同 clientMsgId）都会到这里
				utils.Warn(context.Background(), "冷存储唯一冲突被跳过：排查多实例 snowflake.workerId 重复，或 Redis 丢失后 Kafka 重放（由 uk_conv_cmid 拦截）",
					"conflict_count", int64(len(msgs))-r.RowsAffected, "batch_size", len(msgs))
			}
			return nil
		}
	}
	utils.Error(context.Background(), "cold persist MySQL batch failed", "size", len(batch), "error", err)
	return err
}

// commitFrom 把已 flush 组的全部输入消息（含被丢弃/重放帧——它们的 offset 也必须提交，毒消息不占分区）
// 按分区取最高位推进为组提交 offset（跨协程提交由 kafka-go offsets store 内部互斥保证）。
// 提交失败只告警不重试：下次重启/重平衡从旧 offset 重放，幂等门禁（cm）保证不双落——宁可重放一次，不可卡死分区。
func commitFrom(msgs []utils.KafkaMsg) {
	maxByPart := make(map[int]utils.KafkaMsg, 2)
	for _, m := range msgs {
		if cur, ok := maxByPart[m.Partition]; !ok || m.Offset > cur.Offset {
			maxByPart[m.Partition] = m
		}
	}
	toCommit := make([]utils.KafkaMsg, 0, len(maxByPart))
	for _, m := range maxByPart {
		toCommit = append(toCommit, m)
	}
	if len(toCommit) == 0 {
		return
	}
	if err := utils.CommitKafka(toCommit...); err != nil {
		utils.Warn(context.Background(), "cold writer commit 失败（重启后重放由幂等兜底）", "error", err)
	}
}

// StopColdWriter 通知冷存 worker 排空并落完最后一批（含投递/ACK/提交收尾），阻塞至其真正退出（最长 10s）
func StopColdWriter() {
	if coldStop == nil {
		return
	}
	coldStop <- struct{}{} // 与 worker 握手：它能收到即意味着它将开始收尾
	select {
	case <-coldDone:
	case <-time.After(10 * time.Second):
		utils.Warn(context.Background(), "cold writer 收尾超时，可能有在途消息未落库/未投递")
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
