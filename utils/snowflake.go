package utils

import (
	"context"
	"errors"
	"sync"
	"time"
)

// 特制 53 Bits 雪花算法 (为了适配前端 JS 安全整数范围和 Redis ZSet 的 Float64 Score)
// 结构：41位时间戳 + 5位机器码(最多32台) + 7位序列号(每毫秒最多128个ID)
const (
	workerBits  uint8 = 5
	numberBits  uint8 = 7
	workerMax   int64 = -1 ^ (-1 << workerBits)
	numberMax   int64 = -1 ^ (-1 << numberBits)
	timeShift   uint8 = workerBits + numberBits
	workerShift uint8 = numberBits
	// 自定义纪元时间(2024-01-01，可往后使用69年)
	epoch       int64 = 1704067200000
)

type Worker struct {
	mu        sync.Mutex
	timestamp int64
	workerId  int64
	number    int64
}

var sfWorker *Worker

func InitSnowflake(workerId int64) error {
	if workerId < 0 || workerId > workerMax {
		return errors.New("Worker ID exceed maximum constraint")
	}
	sfWorker = &Worker{
		timestamp: 0,
		workerId:  workerId,
		number:    0,
	}
	return nil
}

// GenSnowflakeId 生成全局唯一的且在 Redis 和 前端 中不丢失精度的长整型递增 ID
func GenSnowflakeId() int64 {
	if sfWorker == nil {
		InitSnowflake(1) // 如果没初始化，默认机器号为1
	}
	sfWorker.mu.Lock()
	defer sfWorker.mu.Unlock()

	now := time.Now().UnixNano() / 1e6
	// 时钟回拨防御（B3）：小回拨（≤2s）有界自旋等追上并告警一次；
	// 大回拨"借时不回头"——沿用旧时间戳继续推进序列，保证 ID 单调、绝不长时间卡死发号路径。
	if now < sfWorker.timestamp {
		offset := sfWorker.timestamp - now
		if offset <= 2000 {
			Warn(context.Background(), "时钟回拨，有界等待追平", "offset_ms", offset)
			for now < sfWorker.timestamp {
				time.Sleep(time.Duration(sfWorker.timestamp-now) * time.Millisecond)
				now = time.Now().UnixNano() / 1e6
			}
		} else {
			Warn(context.Background(), "时钟大幅回拨，借时间戳维持 ID 单调", "offset_ms", offset)
			now = sfWorker.timestamp
			sfWorker.number++
			if sfWorker.number > numberMax {
				sfWorker.number = 0
				sfWorker.timestamp++
				now = sfWorker.timestamp
			}
			ID := int64((now-epoch)<<timeShift | (sfWorker.workerId << workerShift) | (sfWorker.number))
			return ID
		}
	}
	if sfWorker.timestamp == now {
		sfWorker.number++
		if sfWorker.number > numberMax {
			// 当前毫秒内序列号用完，自旋等待下一毫秒
			for now <= sfWorker.timestamp {
				now = time.Now().UnixNano() / 1e6
			}
			// 必须换纪元重置：否则 number 携带 128 溢出进 worker 位段（ID 位模式与 "workerId+1 的 0 号 ID" 雷同，多实例部署下就是撞号炸弹）
			sfWorker.timestamp = now
			sfWorker.number = 0
		}
	} else {
		sfWorker.number = 0
		sfWorker.timestamp = now
	}
	// 拼接并返回
	ID := int64((now-epoch)<<timeShift | (sfWorker.workerId << workerShift) | (sfWorker.number))
	return ID
}
