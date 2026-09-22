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
	if _, err := pipe.Exec(ctx); err != nil {
		fmt.Println("Redis hot persist Error:", err)
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

// MarkRead 打开会话即已读：清未读计数并记录 seq 游标（多端同步预留）
func MarkRead(userId int64, convId string) {
	if convId == "" {
		return
	}
	ctx := context.Background()
	pipe := utils.Red.Pipeline()
	pipe.HDel(ctx, unreadKeyPrefix+strconv.FormatInt(userId, 10), convId)
	if max, err := utils.Red.Get(ctx, seqKeyPrefix+convId).Result(); err == nil {
		pipe.Set(ctx, readKeyPrefix+strconv.FormatInt(userId, 10)+":"+convId, max, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		fmt.Println("MarkRead error:", err)
	}
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
