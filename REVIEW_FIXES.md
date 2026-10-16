# 并发审查修复记录（REVIEW_FIXES）

> 来源：对 Hub/消息管线/Kafka/雪花/鉴权/时间轮/群缓存/已读/前端/停机十项的对抗性审查。
> 判定基线：10 组发现中 6 组真问题、2 组半对（药方修正）、2 组误报（已在文末反驳）。
> 状态：代码全部落地，`go build/vet` 通过；运行时验收见 §验证结果。

## A 类（真 bug，必修）

### A1 心跳时间轮任务多端共享键 → 按设备隔离（message.go / hub.go 无关）
- **缺陷**：`TW.AddTask(delay, userId, job)` 以 userId 为键。多端时任一设备心跳会替换任务闭包（捕获最后心跳者的 node）：静默设备"借"活跃设备的命，而超时触发时踢的是**最后续期的那台**（误杀活端、留尸静默端）。
- **修复**：键改 `hbKey(node)="hb:{uid}#{deviceId}"`；三处统一（注册 / 心跳续期 / defer RemoveTask）。
- **代数守卫**：`Node.hbGen atomic.Int64`，续期 +1；超时 job 执行前比对 `node.hbGen.Load() != gen → return`，消灭"tick 弹出任务与刚到达的心跳续期毫秒级交错"的残余竞态（job 异步执行，TimeWheel 单协程遍历防不了这个）。
- 命名空间注：时间轮键新增 `hb:` 前缀（与 `double_delete_` 并列，见 CLAUDE.md）。

### A7 优雅停机顺序（main.go / utils/kafka.go）
- **缺陷**：旧序 `CloseKafka(先关 producer/writer) → StopColdWriter`。hijack 后的 WS 不受 `srv.Shutdown` 管辖，收尾期在线用户仍在发消息 → producer 已关 → `produceOrDefer` 全数失败 → 不必要的 -1 ACK 风暴；且消费者协程退出与否无握手。
- **修复**：新序 `srv.Shutdown → StopKafkaConsumer(信号+等 consumerDone ≤10s+关reader) → StopColdWriter(排空=落库+投递+ACK) → CloseKafkaProducer`。
- 配套（并入原 B6）：`kafkaConsumerDone` 退出握手；消费者若在 `enqueueCold` 等待中收到停机信号 → **HDEL 去重占位 + 返回 error（offset 不提交）**→ 重启后 Kafka 重放补齐，不丢不重。
- 澄清对审查的一处纠正：审查称"消费者可能丢手中消息"不成立（fetch→handler→commit 单协程串行，无抢占点）；本次改动收益是**可证明的停机序**而非丢消息修复。

### A2 冷写失败回滚热层副本（deliver.go failLanded）
- **缺陷**：热 ZAdd 成功、冷写重试耗尽后仅清 cm 标记+死信+ACK-1，**热层残留一条冷层没有的"幽灵消息"**，违背"冷层为全集"不变量，会泄漏进热层读结果。
- **修复**：`ZRem(convId, string(raw))`（按 member 精确删）+ `ZRemRangeByScore(seq,seq)` 兜底，再清标记。
- seq 空洞说明（面试口径）：INCR 不回滚，序列出现空洞但语义无损——游标只比大小、排序不受影响、重入的 clientMsgId 拿新 seq 落新行，前端按 clientMsgId 渲染去重。

### A4 并发 MarkRead 双计群 read_count（store.go）
- **缺陷**：`old` 在事务外读取。同用户双端并发 markRead → 都读到旧水位 → `(old,seq]` 窗口 `read_count+1` 执行两遍。"水位单调防重计"的前提（读-改-写原子）不成立。
- **修复**：`markReadWindowTx` 事务串行：先"占位 upsert"（`ON DUPLICATE last_seq=last_seq`）保证行存在并持 X 锁——**规避对不存在行 SELECT FOR UPDATE 的间隙锁死锁**（RR 下两个首读事务互等再插同一行）→ `SELECT FOR UPDATE` 取权威 old → 窗口 +1 → 前移水位 → 提交。`seq<=old` 直接空转返回（双端后到者窗口自然为空）。
- 私聊分支不变（read_time 回填本身幂等）。

### A3 群成员缓存击穿风暴 + 负缓存 + TTL（deliver.go）
- **缺陷**：miss 时每条群消息裸打 MySQL（Redis 重启即风暴）；TTL 24h 使成员变更不一致窗口过长；`CacheGroupMember` 裸 SAdd 可建**无 TTL 的僵尸键**。
- **修复**：`singleflight` 每群并发只放一个回源；负缓存哨兵成员 `"0"`（uid 恒 >0）+ 30s TTL；正缓存 TTL 24h→**15min**（100 群×高频也仅 ~0.1 QPS 回源）；`CacheGroupMember` 改管道：`SRem 哨兵 + SAdd + Expire`（顺带修复"刚加群被空集负缓存挡 30s"）；预留 `InvalidateGroup(gid)`——**未来加退群/解散必须调用**。

### A5 前端三项（foot.html）
1. **seenIds 全局 500 清空会误杀** → 改**按 convId 分桶**，每桶 200 按插入序淘汰最旧键；桶键取 `(Type1/2? convIdOf(data.Type, data.TargetId) : currentConvId)`（业务帧不带 ConvId，与后端 convIdOf 同规则推导）。
2. **`indexOf("}")` 判 JSON 脆弱** → `charAt(0)==='{'` + `try/catch JSON.parse`。
3. **onclose 只打日志，承诺的 pending 补发永不触发** → 退避自动重连（1s 起指数、上限 15s、≤10 次，成功 onopen 计数清零；code 1000/1001 视为正常不重连）；超限 toast 引导刷新。

### A6 设备数上限（hub.go / message.go / config）
- **新增** `hub.maxDevices`（缺省 5）：probe 通道扩展为返回"除本 did 外设备数"，`Chat()` 在 **Upgrade 前**软拒 403（体面原因，同设备重连不受限）；`register` 内硬兜底——竞态窗口挤进来的超限新节点直接 `close(DataQueue)`（sendProc 关 TCP、recvProc 自清、unregister 按指针找不到即无害）。
- probe 超时按放行（-1）处理：宁多一台，不可在 Hub 繁忙时误拒。
- **反驳审查一点**：deviceId 是客户端自由字符串，上限只防"并发设备数"不防伪造——真正防线是会话 token 属主校验；两者组合才闭环。

## B 类（加固）

| 项 | 修复 |
|---|---|
| B1 冷管道停摆可观测 | `enqueueCold`：入队等待每 5s 打【警告】coldCh 水位 + 响应停机；monitor 新增 `冷队列: n/8192`。**设计坚持**：停摆时阻塞消费者是正确行为（Kafka 天然缓冲、offset 未提交靠重放），只补声音不改道。 |
| B3 时钟回拨 | 小回拨(≤2s)有界自旋+告警；大回拨**借时不回头**（沿用 lastTs 推进序列，ID 保单调），拒绝"自旋一分钟"与"panic 炸进程"两种极端。 |
| B4 重放最终防线 | `message` 加复合唯一 **`uk_conv_cmid(conv_key, client_msg_id)`**（GORM priority tag 对齐 SQL 快照列序）。审查反驳成立的部分：全局唯一 clientMsgId 会因跨用户撞号误杀 → 降为会话内复合；空串行（仅 UDP 旁路）同会话至多一条，可接受。cold writer 告警文案区分"workerId 撞号 / Redis 丢失后重放被 uk 拦截"。 |
| B5 会话撤销即时生效 | `TouchToken` 返回 bool（Expire 命中性；Redis 瞬时错误返回 true 不误杀）；心跳检测到 false → 断开该设备端（旧连接不再骑在失效会话上）。dev 跳鉴权模式下跳过检查。 |
| B7 分片哈希 | `GetHub` 改黄金比例混位 `(uid*0x9E37..7C15)>>60`：连续自增 id 本就均匀（审查称"奇偶集中 8 片"是 mod-2 的误解），但**低位有规律的号段**（步长 16 等外部 id）会砸一个分片——一行免疫。分片数仍固定 16。 |
| 顺手修 | `ClientCount` 原从 monitor goroutine 遍历各分片 `clients`（与 Run 的数据竞争）→ 分片内 `nodeCount atomic.Int64`，Run 内增删，外部只读原子值。 |

## 明确不做（含理由）

- **B2 workerId Redis 租约**：多实例部署时才需要的防线（当前单实例），留 `【待办-多实例】`；届时 `SET worker:lease:{id} NX EX` + 续租协程，冲突**拒绝启动**而非事后警告。
- **WS token 走 query**：浏览器 API 不支持自定义头，属既定妥协（生产方案：Sec-WebSocket-Protocol / 首帧认证 + Nginx 日志脱敏），写入口径不写代码。
- **RequireAll≠真高可用**：单 broker 下等价 min.insync=1；生产清单（3 broker + min.insync.replicas=2 + partitions>1）记入 README 面试口径。当前 topic 单分区也是消费并行度上限。
- **A4 的 Redis 锁替代方案**：审查建议的 `SETNX markread 锁` 有 TTL 过期与临界区赛跑的残余窗口，事务方案是严格占优，采纳事务。

## MQ 流水线优化轮（2026-10-15，基于"背压 vs 推拉"方案评审）

| 项 | 内容 | 验证 |
|---|---|---|
| **提交权移交** | batch 档消费者入队即返回 `ErrCommitDeferred` 继续 fetch；冷 writer 每批落库+投递收口后跨协程 `CommitKafka`（FIFO ⇒ 已处理连续前缀）；批级 panic recover（整批失败收口+照常提交，writer 不静默死亡）；`rec.done` 同步等待拆除 | ack 档 300 连接：完成 ~460/s，LAG 稳定跟随在途窗口，冲突警告 0，dup=幂等回补路径真实运转 |
| **Produce Key=convId** | 同会话恒同分区——多分区扩展下 seq 单写者前提的硬依赖（当前 1 分区也先立规矩）；`kafka.partitions` 配置化（仅影响首建） | 编译级（单分区行为不变） |
| **入口准入三态闸** | `pipelinePressure`=max(coldCh,ppQueue 占用)；绿<0.6 放行、黄 0.6~0.85 线性节流（只延迟不丢）、红≥0.85 即时 `-1 server_busy`；前端对该 reason 改 5s/10s/20s 指数退避 + **pending 落 localStorage 跨刷新幸存** | 拔 MySQL 演练：红区拒收 22143 条、恢复后拒收停增、ACK 回正、LAG 单调收敛；慢端被 DataQueue 背压单端踢线（设计行为）复现 |
| **停机定序修订** | reader 关闭从 StopKafkaConsumer 挪到最后一步 `CloseKafka`——冷 writer 排空收尾需要活着 reader 提交 | 代码审查（Windows taskkill 非优雅信号，Ctrl+C 场景留前台人工确认） |

**运行时观察（下一步优化线索）**：饱和吞吐 ~460/s 的瓶颈是消费者**逐条串行 ~5 次 Redis RTT**（Docker Desktop NAT 下每次 ~0.3ms+）——下一步可做"批内同会话 Redis 操作合并 + 门禁终值化挪到 flush 后"，理论再抬一个数量级。

**验证后清理**：bench 冷层/热层/未读/cm/seq 键全删；`chat_messages` topic 删除（防 taskkill 后残留 1445 条未提交重放复活脏数据），服务下次启动 ensureTopic 自动重建。

## 批内 Redis 合并轮（2026-10-15，"聚合器+落地合一管道"）

| 变更 | 说明 | 实测 |
|---|---|---|
| **消费聚合器**（models/consume.go 新增） | 消费者 fetch 即入 `consumeCh`；2ms/500 条窗口攒批：门禁 HSetNX **一批一 Pipeline**、每会话 **INCRBY n** 批量发号（到达序映射连续 seq，status 数组保序回填）、批内 (conv\|cmid) 本地去重；`coldUnit{recs, commitTo}` 进 unit 通道（64 深，满=停摆背压同旧语义） | 100 连接闭环 **1275/s**（旧串行 460/s 的 2.8×），**丢包 0 / LAG 0 / 热层无洞期运行正常** |
| **门禁终值挪到 flush 后** | "P" 占位在聚合器；终值 `seq:msgId` 与热层 ZAdd/修剪/未读 HINCRBY **合并为 landAndDeliver 单 Pipeline**，冷层成功+exec 成功才扇出（原文→Type4 带权威计数→ACK）；"落库未投递"崩溃窗口的重放见 "P" 继续落库，由唯一索引幂等兜底 | 双开重发副本 ack 补发路径（dup 计数）与旧行为一致 |
| **落库前幂等对账 splitAlreadyLanded** | 发现终值后移引入的新竞态：INSERT 成功~终值写出的窗口内，副本穿过门禁混入后续 unit——直接插撞 `uk_conv_cmid`。writer 每批先做 ①跨 unit 批内 seen 去重 ②DB 已存行查询→副本转"重放"（回补原 seq/msgId ACK，不污染热层/未读/终值） | 行层证据：(conv,cmid)/(conv,seq) 重复均为 **0** |
| **inline 档并轨** | inline = unit 窗口 0/批 1 的同路径特例，删掉第二套写法 | 冷读路径行为不变 |
| **bench v3 拒收冷却**（test/bench_ws.go） | ack 模式收到 server_busy 后每连接 300ms 冷却再补窗——否则压测客户端自己成洪泛器（此前 30 秒刷出 62 万 attempted 假数据） | storm 消失，attempted≈完成 |
| 运维注记 | **topic 不可在服务运行中删除**（kafka-go Reader 对删除重建会静默挂住，本会话实测踩坑），停机窗口清理或重启服务；`conflict_count:1/批` 的告警在副本场景可能由合法重放触发（文案已含双病因） | — |

**残留边缘（如实记录，未修完）**：mq6 终验仍有 ~0.2% 唯一冲突告警伴生**无害 seq 洞**（丢的只有编号，非数据：行层无重复无缺失、LAG 0、丢包 0）。疑点收敛到雪花"借时"分支跨 unit 边界与启动回放交叠，需要专项复现（构造时钟回拨+副本注入）。影响评级：仅游标美观/告警噪音，无正确性损伤；列入待办。

## 验证结果（运行时验收，全部通过）

环境：compose redis/kafka/mysql + 原生服务端注入 `TIMEOUT_DELAYHEARTBEAT=8 HUB_MAXDEVICES=2`，真实 token 全链路（不开 dev 跳鉴权）。

| 用例 | 结果 |
|---|---|
| **A1 多端心跳隔离**：B→A 双端 phase1 齐达；A-dev1 沉默 8s 被踢、A-dev2 持续心跳**存活且继续收 phase2** | ✅ `phase1=true/true devA1被踢=true devA2存活可收=true`（旧共享键代码必误杀活跃端） |
| **A6 设备上限**：max=2 时第 3 台设备握手 | ✅ `403 拒绝`；同设备重连不受限（phase 逻辑隐含） |
| **A4 并发 MarkRead**：5 条群消息，B 双进程并发 markRead | ✅ `5 条全 read_count=1，双计 0 条`（旧实现会双计） |
| **A2 冷失败回滚**：停 MySQL 发 3 条 → 3×ACK(-1)+死信 3 条+**热层零残留**；恢复后发 2 条 → 2×ACK(0)，热层/冷层恰 4（=此前 hb 用例 2 条+本次 2 条，故障 3 条未污染） | ✅ 全链路一致 |
| **B5 会话续期反查** | ✅ 隐式通过：hb 用例 dev2 心跳 12s+ 无误断（alive=false 才断） |
| 管线回归（seq/热冷/ACK/unread/群扇出/g_members 缓存） | ✅ 上述用例全程走真实管线无异常 |

**未在运行时覆盖（如实标注）**：A7 停机定序（Windows 下 `taskkill` 非 SIGINT，Ctrl+C 场景需前台手动验证一次：观察 `final flush+deliver done` 与无 -1 ACK 洪泛）；B1 停摆【警告】需打满 8192 队列才触发（设计路径已代码审查）；bench v3 相关不在本轮。
