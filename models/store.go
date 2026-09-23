package models

// 消息冷热存储与会话状态层：
//   会话 convId：私聊 "p_{小id}_{大id}"、群聊 "g_{群id}"（convIdOf 是唯一来源），
//   热层/冷层/未读数/事件推送共用，杜绝用户id与群id串号。
//   热层 = Redis ZSet（score = 会话级递增 seq，单会话修剪 + 滑动 TTL）
//   冷层 = MySQL message 表（主键 id = 雪花 msgId，(conv_key, seq) 唯一，游标分页）
//   未读 = Redis HASH unread:{uid}（field=convId），markRead 清除并记录 read 游标

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

// coldRecord 冷存储待写队列元素
type coldRecord struct {
	msg Message
}

var coldCh = make(chan coldRecord, 8192)

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

// persistMessage Kafka 消费者回调：发 seq → 热层 → 冷层 → 未读维护与事件推送
func persistMessage(msgData []byte) {
	jsonMsg := Message{}
	// 非 JSON（系统提示语等误入链路）直接跳过，防止污染会话 key（历史上 msg_0_0 就是这么来的）
	if err := json.Unmarshal(msgData, &jsonMsg); err != nil {
		fmt.Println("persistMessage skip invalid payload:", err)
		return
	}

	convId := convIdOf(jsonMsg.Type, int64(jsonMsg.UserId), jsonMsg.TargetId)
	msgId := utils.GenSnowflakeId()
	ctx := context.Background()

	// clientMsgId 幂等去重：Kafka 重放 / 客户端重发直接整体跳过。
	// 去重集随会话维护，超过 2×hotKeep 时随机淘汰——只承诺"近期窗口"内的重放去重（远期重复靠主键冲突兜底）。
	if jsonMsg.ClientMsgId != "" {
		if has, err := utils.Red.SIsMember(ctx, "cm:"+convId, jsonMsg.ClientMsgId).Result(); err == nil && has {
			fmt.Println("persistMessage skip duplicate clientMsgId:", jsonMsg.ClientMsgId)
			return
		}
	}

	// 会话级严格递增 seq（INCR 原子；消费者是当前唯一写入点）
	seq, err := utils.Red.Incr(ctx, seqKeyPrefix+convId).Result()
	if err != nil {
		fmt.Println("seq INCR failed, skip persist:", convId, err)
		return
	}

	// 热层：一次 Pipeline 完成 写入(score=seq)+修剪+滑动过期
	hotKeep := int64(cfgInt("storage.hotKeep", 200))
	ttl := time.Duration(cfgInt("storage.hotTTLDays", 7)) * 24 * time.Hour
	pipe := utils.Red.Pipeline()
	// 保留最原生字节流做 Member，避免重新序列化的 CPU 损耗
	pipe.ZAdd(ctx, convId, redis.Z{Score: float64(seq), Member: msgData})
	pipe.ZRemRangeByRank(ctx, convId, 0, -(hotKeep + 1))
	pipe.Expire(ctx, convId, ttl)
	if jsonMsg.ClientMsgId != "" {
		ck := "cm:" + convId
		pipe.SAdd(ctx, ck, jsonMsg.ClientMsgId)
		pipe.Expire(ctx, ck, ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		fmt.Println("Redis hot persist Error:", err)
	}
	if jsonMsg.ClientMsgId != "" {
		// 近似淘汰超窗的去重集（随机弹出可接受：被弹条目若重放，落到冷层靠 msgId 冲突/告警兜底）
		ck := "cm:" + convId
		if n, err := utils.Red.SCard(ctx, ck).Result(); err == nil && n > 2*hotKeep {
			utils.Red.SPopN(ctx, ck, n-2*hotKeep)
		}
	}

	// 冷层：投入批量落盘队列（channel 满则反压消费协程，宁慢勿丢）
	coldCh <- coldRecord{
		msg: Message{
			Model:      gorm.Model{ID: uint(msgId)},
			UserId:     jsonMsg.UserId,
			TargetId:   jsonMsg.TargetId,
			Type:       jsonMsg.Type,
			Media:      jsonMsg.Media,
			Content:    jsonMsg.Content,
			CreateTime: jsonMsg.CreateTime,
			Pic:        jsonMsg.Pic,
			Url:        jsonMsg.Url,
			Desc:       jsonMsg.Desc,
			Amount:     jsonMsg.Amount,
			Seq:        seq,
			ConvKey:    convId,
			ClientMsgId: jsonMsg.ClientMsgId,
		},
	}

	// 未读数：私聊给接收方；群聊给除发送者外的全体成员
	if jsonMsg.Type == 2 {
		members := SearchUserByGroupId(uint(jsonMsg.TargetId))
		targets := make([]int64, 0, len(members))
		for _, m := range members {
			if int64(m) != int64(jsonMsg.UserId) {
				targets = append(targets, int64(m))
			}
		}
		pushUnread(convId, seq, targets)
	} else if jsonMsg.TargetId != int64(jsonMsg.UserId) {
		pushUnread(convId, seq, []int64{jsonMsg.TargetId})
	}
}

// pushUnread 未读 +1 并向在线用户推送 Type=4 事件
func pushUnread(convId string, seq int64, uids []int64) {
	ctx := context.Background()
	for _, uid := range uids {
		uk := unreadKeyPrefix + strconv.FormatInt(uid, 10)
		n, err := utils.Red.HIncrBy(ctx, uk, convId, 1).Result()
		if err != nil {
			fmt.Println("unread HIncrBy error:", uk, err)
			continue
		}
		evt, err := json.Marshal(map[string]interface{}{"Type": 4, "ConvId": convId, "Seq": seq, "Unread": n})
		if err != nil {
			continue
		}
		// 仅内存推送，不落任何存储；离线用户下次 unreadList 拉全量补偿
		deliverLocal(uid, evt)
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

// ReadCursor 会话已读游标落库表（Redis read: 键的持久账本；群聊回执的水位模型）
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
//  1. Redis 游标 + 清未读；2. 游标 upsert 落库；
//  3. 私聊回填对方消息的 read_time（群聊只记成员水位，逐条全员回执留后续）；
//  4. 私聊向对端在线设备推 Type=5 已读回执事件。
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

	// 私聊：把游标之前、对方发给我的消息回填 read_time
	if strings.HasPrefix(convId, "p_") {
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
	}
}

// PeerReadSeq 会话对端的已读游标（私聊气泡"已读"状态用）：Redis 优先，缺则回落 DB
func PeerReadSeq(convId string, uid int64) int64 {
	peer := convPeer(convId, uid)
	if peer <= 0 {
		return 0
	}
	ctx := context.Background()
	if v, err := utils.Red.Get(ctx, readKeyPrefix+strconv.FormatInt(peer, 10)+":"+convId).Result(); err == nil {
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	}
	var rc ReadCursor
	if err := utils.DB.Where("user_id = ? AND conv_key = ?", peer, convId).First(&rc).Error; err == nil {
		return rc.LastSeq
	}
	return 0
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

// StartColdWriter 冷存储批量落盘 worker：攒够 coldBatchSize 条或等 coldFlushMs 毫秒即写 MySQL
// 必须在 utils.InitMysql() 之后、Kafka 消费者启动之前调用
func StartColdWriter() {
	coldStop = make(chan struct{})
	coldDone = make(chan struct{})
	batchSize := cfgInt("storage.coldBatchSize", 100)
	flush := time.Duration(cfgInt("storage.coldFlushMs", 200)) * time.Millisecond

	go func() {
		batch := make([]Message, 0, batchSize)
		ticker := time.NewTicker(flush)
		defer ticker.Stop()

		flushBatch := func() {
			if len(batch) == 0 {
				return
			}
			// 主键/唯一索引冲突时跳过（幂等；冲突规模异常会在下方告警暴露）
			res := utils.DB.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(batch, batchSize)
			if res.Error != nil {
				// 整批失败只记日志不重试（至少一次语义下可容忍极少量缺口，换取消费不被卡死）
				fmt.Println("cold persist MySQL batch failed, size:", len(batch), "err:", res.Error)
			} else if res.RowsAffected < int64(len(batch)) {
				// 冲突跳过的行要大声告警：多实例 workerId 重复或 Kafka 重放会在这里丢消息
				fmt.Printf("【警告】冷存储 %d/%d 条主键冲突被跳过：检查多实例 snowflake.workerId 是否重复！\n",
					int64(len(batch))-res.RowsAffected, len(batch))
			}
			batch = batch[:0]
		}

		for {
			select {
			case rec := <-coldCh:
				batch = append(batch, rec.msg)
				if len(batch) >= batchSize {
					flushBatch()
				}
			case <-ticker.C:
				flushBatch()
			case <-coldStop:
				// 排空在途队列后做最后一次落盘再退出（宁慢勿丢）
				for {
					select {
					case rec := <-coldCh:
						batch = append(batch, rec.msg)
						if len(batch) >= batchSize {
							flushBatch()
						}
					default:
						flushBatch()
						fmt.Println("cold writer stopped, final flush done")
						close(coldDone)
						return
					}
				}
			}
		}
	}()
	fmt.Println("cold writer started, batchSize:", batchSize, "flushInterval:", flush)
}

// StopColdWriter 通知冷存 worker 排空并落完最后一批，阻塞至其真正退出（最长 10s）
func StopColdWriter() {
	if coldStop == nil {
		return
	}
	coldStop <- struct{}{} // 与 worker 握手：它能收到即意味着它将开始收尾
	select {
	case <-coldDone:
	case <-time.After(10 * time.Second):
		fmt.Println("【警告】cold writer 收尾超时，可能有在途消息未落库")
	}
}

// LoadHistory 冷层游标分页读取：按 conv_key 取 seq < beforeSeq 的最近 size 条（seq 降序）
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
