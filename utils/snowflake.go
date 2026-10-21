package utils

import (
	"context"
	"errors"
	"sync"
	"time"
)

// 53-bit 雪花算法（适配 JS 安全整数 + Redis ZSet Float64 Score）。
// 结构：41 位时间戳 + 5 位机器码（≤32 台）+ 7 位序列号（每毫秒 ≤128 个 ID）。
const (
	workerBits  uint8 = 5
	numberBits  uint8 = 7
	workerMax   int64 = -1 ^ (-1 << workerBits)
	numberMax   int64 = -1 ^ (-1 << numberBits)
	timeShift   uint8 = workerBits + numberBits
	workerShift uint8 = numberBits
	epoch       int64 = 1704067200000 // 2024-01-01
)

// Worker 雪花发号器。
type Worker struct {
	mu        sync.Mutex
	timestamp int64
	workerId  int64
	number    int64
}

var sfWorker *Worker

// InitSnowflake 初始化全局发号器（workerId 越界返回错误）。
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

// GenSnowflakeId 生成全局唯一递增 ID。
// 时钟回拨防御：小回拨（≤2s）有界自旋；大回拨"借时不回头"沿用旧时间戳推进序列，保证 ID 单调。
func GenSnowflakeId() int64 {
	if sfWorker == nil {
		InitSnowflake(1)
	}
	sfWorker.mu.Lock()
	defer sfWorker.mu.Unlock()

	now := time.Now().UnixNano() / 1e6
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
			return int64((now-epoch)<<timeShift | (sfWorker.workerId << workerShift) | sfWorker.number)
		}
	}
	if sfWorker.timestamp == now {
		sfWorker.number++
		if sfWorker.number > numberMax {
			for now <= sfWorker.timestamp {
				now = time.Now().UnixNano() / 1e6
			}
			sfWorker.timestamp = now
			sfWorker.number = 0
		}
	} else {
		sfWorker.number = 0
		sfWorker.timestamp = now
	}
	return int64((now-epoch)<<timeShift | (sfWorker.workerId << workerShift) | sfWorker.number)
}
