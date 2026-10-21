package utils

import (
	"container/list"
	"time"
)

// TW 全局延迟任务时间轮（1s 精度 × 60 槽，init 自启）。
var TW *TimeWheel

// Task 延迟任务。
type Task struct {
	delay  time.Duration
	circle int
	key    interface{}
	job    func()
	pos    int
}

// TimeWheel 时间轮：O(1) 添加/删除/刷新，替代海量连接心跳的 O(N) 遍历。
type TimeWheel struct {
	interval time.Duration
	ticker   *time.Ticker
	slots    []*list.List

	currentPos int
	slotNum    int

	addTaskChannel    chan *Task
	removeTaskChannel chan interface{}
	stopChannel       chan bool

	taskMap map[interface{}]*list.Element
}

func init() {
	TW = NewTimeWheel(1*time.Second, 60)
	TW.Start()
}

// NewTimeWheel 创建时间轮（不调用 Start，需手工启动）。
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
	tw.currentPos = (tw.currentPos + 1) % tw.slotNum

	for e := l.Front(); e != nil; {
		task := e.Value.(*Task)
		if task.circle > 0 {
			task.circle--
			e = e.Next()
			continue
		}

		go task.job()

		next := e.Next()
		l.Remove(e)
		if tw.taskMap[task.key] == e {
			delete(tw.taskMap, task.key)
		}
		e = next
	}
}

// AddTask 添加或刷新延迟任务（同 key 覆盖旧任务）。
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

// RemoveTask 取消延迟任务。
func (tw *TimeWheel) RemoveTask(key interface{}) {
	tw.removeTaskChannel <- key
}

func (tw *TimeWheel) addTask(task *Task) {
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
	delayMs := d.Milliseconds()
	intervalMs := tw.interval.Milliseconds()
	if intervalMs <= 0 {
		intervalMs = 1
	}
	circle = int(delayMs / intervalMs / int64(tw.slotNum))
	pos = (tw.currentPos + int(delayMs/intervalMs)) % tw.slotNum
	return
}
