package utils

import (
	"container/list"
	"time"
)

// 全局唯一的延迟任务时间轮
var TW *TimeWheel

// Task 延时任务
type Task struct {
	delay  time.Duration
	circle int
	key    interface{}
	job    func()
	pos    int // 记住自己所在的槽位，实现 O(1) 删除
}

// TimeWheel 时间轮（解决海量连接心跳遍历的 O(N) 性能瓶颈）
type TimeWheel struct {
	interval time.Duration // 刻度时间，比如 1 秒
	ticker   *time.Ticker
	slots    []*list.List  // 环形数组

	currentPos int // 当前指针位置
	slotNum    int // 槽位数量，比如 60 个槽代表 60 秒（1分钟内）

	addTaskChannel    chan *Task
	
	removeTaskChannel chan interface{}
	stopChannel       chan bool

	taskMap map[interface{}]*list.Element // key -> 链表节点，用于 O(1) 查找并刷新/删除
}

func init() {
	// 初始化一个精度为1秒，共60个槽位（支持60秒）的时间轮
	TW = NewTimeWheel(1*time.Second, 60)
	TW.Start()
}

func NewTimeWheel(interval time.Duration, slotNum int) *TimeWheel {
	tw := &TimeWheel{
		interval:          interval,
		slots:             make([]*list.List, slotNum),
		currentPos:        0,
		slotNum:           slotNum,
		addTaskChannel:    make(chan *Task),
		removeTaskChannel: make(chan interface{}),
		stopChannel:       make(chan bool),
		taskMap:           make(map[interface{}]*list.Element),
	}
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
	return tw
}

func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler()
		case task := <-tw.addTaskChannel:
			tw.addTask(task)
		case key := <-tw.removeTaskChannel:
			tw.removeTask(key)
		case <-tw.stopChannel:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) tickHandler() {
	l := tw.slots[tw.currentPos]
	tw.currentPos = (tw.currentPos + 1) % tw.slotNum // 指针往前拨动一格

	for e := l.Front(); e != nil; {
		task := e.Value.(*Task)
		if task.circle > 0 { // 还没到圈数，跳过
			task.circle--
			e = e.Next()
			continue
		}

		// 倒计时已到，执行任务 (异步，防阻塞系统滴答)
		go task.job()

		// 执行后移除任务
		next := e.Next()
		l.Remove(e)
		if tw.taskMap[task.key] == e {
			delete(tw.taskMap, task.key)
		}
		e = next
	}
}

// AddTask 开放给外部调用的添加/刷新任务接口
func (tw *TimeWheel) AddTask(delay time.Duration, key interface{}, job func()) {
	if delay < 0 {
		return
	}
	tw.addTaskChannel <- &Task{
		delay: delay,
		key:   key,
		job:   job,
	}
}

// RemoveTask 取消某个任务
func (tw *TimeWheel) RemoveTask(key interface{}) {
	tw.removeTaskChannel <- key
}

// 内部方法 - 必须在 goroutine 回调里单线程操作，做到无需加锁
func (tw *TimeWheel) addTask(task *Task) {
	// 如果任务已经存在，先删除旧的 (利用这步做到心跳 O(1) 刷新)
	if e, ok := tw.taskMap[task.key]; ok {
		oldTask := e.Value.(*Task)
		tw.slots[oldTask.pos].Remove(e)
	}

	pos, circle := tw.getPosAndCircle(task.delay)
	task.circle = circle
	task.pos = pos

	e := tw.slots[pos].PushBack(task)
	tw.taskMap[task.key] = e
}

func (tw *TimeWheel) removeTask(key interface{}) {
	if e, ok := tw.taskMap[key]; ok {
		oldTask := e.Value.(*Task)
		tw.slots[oldTask.pos].Remove(e)
		delete(tw.taskMap, key)
	}
}

func (tw *TimeWheel) getPosAndCircle(d time.Duration) (pos int, circle int) {
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	circle = delaySeconds / intervalSeconds / tw.slotNum
	pos = (tw.currentPos + delaySeconds/intervalSeconds) % tw.slotNum
	return
}
