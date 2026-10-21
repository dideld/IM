package utils

import (
	"sync"
	"testing"
)

// 并发唯一性 + worker 位段纯净（序列溢出换纪元缺陷的回归护栏）
func TestSnowflakeUniqueAndBitClean(t *testing.T) {
	const goroutines, per = 8, 100000
	if err := InitSnowflake(21); err != nil {
		t.Fatal(err)
	}
	sets := make([]map[int64]bool, goroutines)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		sets[g] = make(map[int64]bool, per)
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				sets[g][GenSnowflakeId()] = true
			}
		}(g)
	}
	wg.Wait()
	seen := make(map[int64]bool, goroutines*per)
	for g := 0; g < goroutines; g++ {
		for id := range sets[g] {
			if seen[id] {
				t.Fatalf("duplicate id %d", id)
			}
			seen[id] = true
			if w := (id >> 7) & 31; w != 21 { // number 溢出进 worker 位段 → 位模式污染
				t.Fatalf("worker bits polluted: id=%d worker=%d", id, w)
			}
			if id <= 0 || id >= 1<<53 { // 53bit 约束（JS 安全整数 & float64 score）
				t.Fatalf("out of 53-bit range: %d", id)
			}
		}
	}
	if int64(len(seen)) != int64(goroutines)*per {
		t.Fatalf("count mismatch: %d", len(seen))
	}
}

// 单毫秒吞吐 >128（numberMax）必然穿越溢出分支，上面的用例已隐式覆盖；
// 这里显式验证溢出后 ID 仍严格大于溢出前（单调性不被"换纪元重置"破坏）。
func TestSnowflakeMonotonicAcrossOverflow(t *testing.T) {
	if err := InitSnowflake(3); err != nil {
		t.Fatal(err)
	}
	prev := GenSnowflakeId()
	for i := 0; i < 5000; i++ { // 必然穿越多个毫秒边界与溢出路径
		id := GenSnowflakeId()
		if id <= prev {
			t.Fatalf("id regressed: %d <= %d at %d", id, prev, i)
		}
		prev = id
	}
}
