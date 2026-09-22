package models

// 消息冷热存储层：
//   热层 = Redis ZSet（单会话最近 N 条，滑动 TTL，低延迟历史读取 /user/redisMsg）
//   冷层 = MySQL message 表（全量归档，按 conv_key+雪花id 游标分页 /user/historyMsg）
// 二者共享 convKeyOf 的会话 key 归一化规则，冷层主键 id 即热层 ZSet score。

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

// convKeyOf 会话 key 归一化的唯一来源：热层 ZSet key 与冷层 conv_key 列同值
func convKeyOf(a, b int64) string {
	x, y := strconv.FormatInt(a, 10), strconv.FormatInt(b, 10)
	if a > b {
		x, y = y, x
	}
	return "msg_" + x + "_" + y
}

// coldRecord 冷存储待写队列元素
type coldRecord struct {
	msg Message
	raw []byte // 出错日志用的原始字节
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

// persistMessage Kafka 消费者回调：热层（Redis ZSet 修剪+TTL）+ 冷层（MySQL 批量落盘队列）
func persistMessage(msgData []byte) {
	jsonMsg := Message{}
	// 非 JSON（如欢迎语等系统提示误入链路）直接跳过，防止污染会话 key（历史上 msg_0_0 就是这么来的）
	if err := json.Unmarshal(msgData, &jsonMsg); err != nil {
		fmt.Println("persistMessage skip invalid payload:", err)
		return
	}

	key := convKeyOf(int64(jsonMsg.UserId), jsonMsg.TargetId)
	msgId := utils.GenSnowflakeId()

	// 热层：一次 Pipeline 往返完成 写入+修剪+滑动过期
	hotKeep := int64(cfgInt("storage.hotKeep", 200))
	ttl := time.Duration(cfgInt("storage.hotTTLDays", 7)) * 24 * time.Hour
	ctx := context.Background()
	pipe := utils.Red.Pipeline()
	// 保留最原生字节流做 Member，避免重新序列化的 CPU 损耗
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(msgId), Member: msgData})
	// 修剪到最近 hotKeep 条（未超限时为空操作）
	pipe.ZRemRangeByRank(ctx, key, 0, -(hotKeep + 1))
	pipe.Expire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		fmt.Println("Redis hot persist Error:", err)
	}

	// 冷层：投入批量落盘队列（channel 满则反压消费协程，宁慢勿丢）
	coldCh <- coldRecord{
		raw: msgData,
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
			ConvKey:    key,
		},
	}
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
			// 主键冲突时跳过（Kafka 重放/重启后至少一次投递，落库需幂等）
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

// LoadHistory 冷层游标分页读取：按 conv_key 取 id < beforeId 的最近 size 条（id 降序）
func LoadHistory(userIdA, userIdB int64, beforeId int64, size int) []Message {
	if size <= 0 {
		size = 20
	}
	if size > 100 {
		size = 100
	}
	ctxDB := utils.DB.Where("conv_key = ?", convKeyOf(userIdA, userIdB))
	if beforeId > 0 {
		ctxDB = ctxDB.Where("id < ?", beforeId)
	}
	var list []Message
	ctxDB.Order("id desc").Limit(size).Find(&list)
	return list
}
