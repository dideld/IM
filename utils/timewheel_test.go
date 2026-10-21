package utils

import (
	"sync/atomic"
	"testing"
	"time"
)

// 时间轮核心语义回归：定时触发 / 同 key 续期只留最新 / RemoveTask 取消。
// 用 10ms×8 槽的小轮，避免慢测试。
func TestTimeWheelScheduleRefreshCancel(t *testing.T) {
	tw := NewTimeWheel(10*time.Millisecond, 8)
	tw.Start()
	defer func() { tw.stopChannel <- true }()

	// 1) 到期触发（delay≈5 ticks）
	var fired atomic.Int64
	tw.AddTask(50*time.Millisecond, "k1", func() { fired.Add(1) })
	time.Sleep(150 * time.Millisecond)
	if fired.Load() != 1 {
		t.Fatalf("want fired once, got %d", fired.Load())
	}

	// 2) 同 key 反复续期：旧任务必须被替换而非叠加执行
	var refreshFires atomic.Int64
	for i := 0; i < 6; i++ { // 每 20ms 续一次，间隔 < delay(60ms)：全程不该触发
		tw.AddTask(60*time.Millisecond, "k2", func() { refreshFires.Add(1) })
		time.Sleep(20 * time.Millisecond)
	}
	if got := refreshFires.Load(); got != 0 {
		t.Fatalf("refreshed task fired during renewal stream: %d", got)
	}
	time.Sleep(120 * time.Millisecond) // 停止续期后应恰好触发一次
	if got := refreshFires.Load(); got != 1 {
		t.Fatalf("want 1 fire after renew stream ends, got %d", got)
	}

	// 3) RemoveTask 取消
	var cancelled atomic.Int64
	tw.AddTask(40*time.Millisecond, "k3", func() { cancelled.Add(1) })
	tw.RemoveTask("k3")
	time.Sleep(150 * time.Millisecond)
	if cancelled.Load() != 0 {
		t.Fatalf("cancelled task fired: %d", cancelled.Load())
	}
}
