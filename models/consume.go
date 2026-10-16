package models

// 消费聚合器（批内 Redis 合并的执行端）：
//   utils 消费者 → consumeCh → 聚合器（窗口攒批：门禁 HSetNX 合并 + 每会话 INCRBY 批量发号）
//   → coldUnit → 冷 writer（落库成功→ 热层+门禁终值+未读 HINCRBY 单 Pipeline → 投递/ACK → 提交）。
//
// 语义要点：
//   - Redis 交互从"每条 4~5 次串行 RTT"降为"每批 2 次 + writer 1 次"，Docker NAT 环境下这是数量级收益；
//   - seq 由聚合器唯一发号（INCRBY n 后按到达顺序分配 end-n+1..end），批内会话有序、跨批 FIFO 全局有序；
//   - 门禁占位 "P" 提前到聚合器，终值 "seq:msgId" 挪到落库成功后与热层同 Pipeline 写——
//     崩溃重放见 cm="P" 视为上次尝试死在中途，继续落库（uk_conv_cmid/uk_conv_seq 幂等兜底）；
//   - 批处理失败（Redis 抖动）→ 有界重试后停摆等待（只出声不丢、不越序），停机信号下放弃该批（offset 未提交靠重启重放）；
//   - 无效帧/压测旁路/重放补 ACK 的 offset 随所属 unit 一并提交（毒消息不占分区的语义保持）。

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"
	"webapp/utils"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
)

// coldUnit 聚合一并处理、一并提交的一批消息：recs=需要落库的存活消息，commitTo=本批**全部**输入消息
// （含被丢弃的无效/旁路/重放帧——它们的 offset 也必须随本 unit 提交，否则分区被毒消息卡住）。
type coldUnit struct {
	recs     []landedRecord
	commitTo []utils.KafkaMsg
}

var (
	consumeCh = make(chan utils.KafkaMsg, 8192)
	coldCh    = make(chan coldUnit, 64) // unit 通道：满即停摆（天然背压），64×500≈3.2 万条缓冲上限
)

// StartKafkaConsumer 启动消费链：utils 消费者（fetch）→ consumeCh → 聚合器 → coldUnit → 冷 writer。
func StartKafkaConsumer() {
	go consumeAggregator()
	utils.StartKafkaConsumer(func(m utils.KafkaMsg) error {
		// 反压：聚合器停摆时这里阻塞（Kafka 天然缓冲、不提交），停机信号下退出
		stall := time.NewTicker(5 * time.Second)
		defer stall.Stop()
		for {
			select {
			case consumeCh <- m:
				return utils.ErrCommitDeferred // 提交权移交消费管线（unit 落库成功后由 writer 推进）
			case <-stall.C:
				utils.Warn(context.Background(), "消费聚合停摆：consumeCh 满（下游 Redis/MySQL 拥塞），检查存储",
					"queue_len", len(consumeCh), "queue_cap", cap(consumeCh))
			case <-time.After(100 * time.Millisecond):
				if utils.KafkaStopping() {
					return context.Canceled
				}
			}
		}
	})
}

// aggParams 聚合窗口与上限；inline 档退化为"每条即一批"（统一代码路径，不再是独立写法）。
func aggParams() (window time.Duration, maxRecs int) {
	if coldModeVal() == "inline" {
		return 0, 1
	}
	ms := viper.GetInt("storage.aggregateWindowMs")
	if ms <= 0 {
		ms = 2
	}
	n := viper.GetInt("storage.aggregateMax")
	if n <= 0 {
		n = 500
	}
	return time.Duration(ms) * time.Millisecond, n
}

func consumeAggregator() {
	window, maxRecs := aggParams()
	timer := time.NewTimer(window)
	timer.Stop()
	collected := make([]utils.KafkaMsg, 0, maxRecs)
	open := false

	flush := func() {
		if len(collected) == 0 {
			return
		}
		batch := collected
		collected = make([]utils.KafkaMsg, 0, maxRecs)
		for { // 失败停摆重试（只出声不丢）；停机信号下整批放弃（未提交→重启重放）
			ok := aggregateBatch(batch)
			if ok {
				break
			}
			if utils.KafkaStopping() {
				utils.Warn(context.Background(), "停机放弃未处理批次（未提交，重启后重放）", "size", len(batch))
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	for {
		select {
		case m := <-consumeCh:
			if !open {
				timer.Reset(window)
				open = true
			}
			collected = append(collected, m)
			if len(collected) >= maxRecs {
				open = false
				timer.Stop()
				flush()
			}
		case <-timer.C:
			open = false
			flush()
		case <-utils.KafkaQuitChan():
			flush() // 尽力把窗口内已收集的做掉（后续 unit 由 writer 排空提交）
			return
		}
	}
}

// aggregateBatch 处理一个聚合窗口内的消息：分类→本地去重→门禁合并→seq 批量发号→组 unit→入队。
// 返回 false 表示 Redis 抖动，调用方停摆重试（本函数不产生副作用丢失：门禁/发号失败时尚未入队）。
func aggregateBatch(batch []utils.KafkaMsg) bool {
	ctx := context.Background()
	unit := coldUnit{commitTo: append([]utils.KafkaMsg(nil), batch...)}

	// ---- 1. 分类：无效帧/非业务类型/压测旁路 直接丢弃（offset 随 unit 提交）----
	type entry struct {
		rec landedRecord
	}
	entries := make([]entry, 0, len(batch))
	seen := make(map[string]bool, len(batch)) // 批内 (conv|clientMsgId) 去重（重发副本可能同窗到达）
	for _, m := range batch {
		msg := Message{}
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			utils.Warn(ctx, "聚合丢弃无效帧", "offset", m.Offset, "error", err)
			continue
		}
		if msg.Type != 1 && msg.Type != 2 {
			continue // 心跳等不该进持久化链路
		}
		if viper.GetBool("storage.skipBenchPersist") && strings.HasPrefix(msg.ClientMsgId, "bench-") {
			continue // 压测旁路：不落库也不 ACK
		}
		rec := landedRecord{
			raw:         m.Value,
			parsed:      msg,
			convId:      convIdOf(msg.Type, int64(msg.UserId), msg.TargetId),
			msgId:       utils.GenSnowflakeId(),
			clientMsgId: msg.ClientMsgId,
			kmsg:        m,
		}
		if rec.clientMsgId != "" {
			dupKey := rec.convId + "|" + rec.clientMsgId
			if seen[dupKey] {
				continue // 批内重复副本：首条保留，其余随 unit 提交丢弃
			}
			seen[dupKey] = true
		}
		entries = append(entries, entry{rec})
	}
	if len(entries) == 0 {
		return enqueueUnit(unit)
	}

	// ---- 2. 门禁合并：一批 HSetNX "P"（无 clientMsgId 的跳过门禁）----
	// 状态数组按 cand 原序标记（keep/drop/ackReplay），保证最终 gated 维持达顺序——
	// gated 乱序会让"按会话分组分配连续 seq"错位（seq 语义=会话内到达序）。
	cand := make([]*landedRecord, 0, len(entries))
	for i := range entries {
		cand = append(cand, &entries[i].rec)
	}
	const (
		sKeep = iota // 无 cmid 或 HSetNX 创建成功
		sGate        // HSetNX 未创建，待二查值
		sDrop        // 终值命中：重放
	)
	status := make([]int, len(cand))
	pipe := utils.Red.Pipeline()
	pipeN := 0
	for i, rec := range cand {
		if rec.clientMsgId == "" {
			status[i] = sKeep
			continue
		}
		pipe.HSetNX(ctx, "cm:"+rec.convId, rec.clientMsgId, "P")
		status[i] = sGate
		pipeN++
	}
	if pipeN > 0 {
		cmders, err := pipe.Exec(ctx)
		if err != nil {
			utils.Error(ctx, "门禁 HSetNX pipeline 失败(停摆重试)", "error", err)
			return false
		}
		gi := 0
		for i := range cand {
			if status[i] != sGate {
				continue
			}
			created, err := cmders[gi].(*redis.BoolCmd).Result()
			gi++
			if err != nil {
				utils.Error(ctx, "门禁结果异常(停摆重试)", "error", err)
				return false
			}
			if created {
				status[i] = sKeep
			}
		}
		// 未创建的批内一次 HGet 定值分类（终值→重放补 ACK；"P"→上次尝试半途，继续）
		need := make([]int, 0, 4)
		pipe2 := utils.Red.Pipeline()
		for i := range cand {
			if status[i] == sGate {
				pipe2.HGet(ctx, "cm:"+cand[i].convId, cand[i].clientMsgId)
				need = append(need, i)
			}
		}
		if len(need) > 0 {
			cmds, err := pipe2.Exec(ctx)
			if err != nil {
				utils.Error(ctx, "门禁 HGet pipeline 失败(停摆重试)", "error", err)
				return false
			}
			for k, i := range need {
				v, _ := cmds[k].(*redis.StringCmd).Result()
				if v != "" && v != "P" && !strings.HasPrefix(v, "P:") {
					status[i] = sDrop
					if parts := strings.SplitN(v, ":", 2); len(parts) == 2 {
						sq, _ := strconv.ParseInt(parts[0], 10, 64)
						mi, _ := strconv.ParseInt(parts[1], 10, 64)
						ackTo(int64(cand[i].parsed.UserId), cand[i].clientMsgId, cand[i].convId, sq, mi, 0, "")
					}
				} else {
					status[i] = sKeep
				}
			}
		}
	}
	gated := make([]*landedRecord, 0, len(cand))
	for i := range cand {
		if status[i] == sKeep {
			gated = append(gated, cand[i])
		}
	}
	if len(gated) == 0 {
		return enqueueUnit(unit)
	}

	// ---- 3. 每会话 INCRBY 批量发号（gated 保持到达序；单聚合器=seq 唯一写入点）----
	byConv := make(map[string][]*landedRecord)
	convOrder := make([]string, 0, 8)
	for _, rec := range gated {
		if _, ok := byConv[rec.convId]; !ok {
			convOrder = append(convOrder, rec.convId)
		}
		byConv[rec.convId] = append(byConv[rec.convId], rec)
	}
	seqPipe := utils.Red.Pipeline()
	for _, cid := range convOrder {
		seqPipe.IncrBy(ctx, seqKeyPrefix+cid, int64(len(byConv[cid])))
	}
	seqCmds, err := seqPipe.Exec(ctx)
	if err != nil {
		utils.Error(ctx, "seq INCRBY pipeline 失败(停摆重试)", "error", err)
		return false
	}
	for i, cid := range convOrder {
		end, err := seqCmds[i].(*redis.IntCmd).Result()
		if err != nil {
			utils.Error(ctx, "seq INCRBY 结果异常(停摆重试)", "conv", cid, "error", err)
			return false
		}
		list := byConv[cid]
		for j, rec := range list { // 到达序映射连续 seq：end-n+1 .. end
			rec.seq = end - int64(len(list)) + int64(j) + 1
			unit.recs = append(unit.recs, *rec)
		}
	}

	// ---- 4. 入队（unit 内保序；unit 间 FIFO ⇒ 提交为连续前缀）----
	return enqueueUnit(unit)
}

// enqueueUnit 入冷队列：unit 通道满=存储管道停摆（writer 被 MySQL 卡住），阻塞+5s【警告】；
// 停机信号下放弃该批（未提交，重启重放）。
func enqueueUnit(unit coldUnit) bool {
	stall := time.NewTicker(5 * time.Second)
	defer stall.Stop()
	for {
		select {
		case coldCh <- unit:
			return true
		case <-stall.C:
			utils.Warn(context.Background(), "冷管道停摆：unit 队列满，writer 拥塞（检查 MySQL），消费者已暂停",
				"units", len(coldCh), "pending_recs", len(unit.recs))
		}
		if utils.KafkaStopping() {
			utils.Warn(context.Background(), "停机放弃未落库批次（未提交，重启重放）", "recs", len(unit.recs))
			return true // 返回 true 结束本批循环（停机由上层 quit 统一收尾）
		}
	}
}

// aggregateTrimConvs 供 writer 收尾：本批涉及的会话去重（cm 修剪按会话一次）。
func distinctConvs(recs []landedRecord) []string {
	seen := make(map[string]bool, 8)
	out := make([]string, 0, 8)
	for i := range recs {
		if !seen[recs[i].convId] {
			seen[recs[i].convId] = true
			out = append(out, recs[i].convId)
		}
	}
	return out
}

// ColdQueueLen 冷队列水位（unit 粒度，monitor/admission 观测用）。
func ColdQueueLen() (int, int) { return len(coldCh), cap(coldCh) }

// consumeQueueLen 聚合入口水位（准入闸信号源之一）
func consumeQueueLen() (int, int) { return len(consumeCh), cap(consumeCh) }
