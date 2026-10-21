# GinChat — Go Web IM

基于 Go 的单体 Web 即时通讯系统：WebSocket 长连接 + 分片 Hub 无锁路由 + 时间轮心跳 + Kafka 异步落盘 + Redis/MySQL 消息冷热双层存储。前端为服务端渲染页面（Layui 风格模板，含私聊/群聊/表情/图片/语音）。

## 技术栈

| 层 | 选型 |
|---|---|
| HTTP / WS | Gin + gorilla/websocket |
| 存储 | MySQL（GORM，冷归档/关系）、Redis（热层消息 ZSet / 用户缓存） |
| 消息队列 | Kafka（segmentio/kafka-go，异步持久化解耦） |
| 配置 | Viper（`config/app.yml`） |
| 部署 | Docker Compose + Nginx（静态资源托管 + WS 反代） |
| 可观测 | pprof、Swagger（`/swagger/index.html`）、内置 10s 状态监控打印 |

## 快速开始

### 依赖

- **一键全栈（推荐，零本地依赖）**：`docker-compose up -d --build` 一次拉起 MySQL(自动建库+初始SQL) + Redis + Kafka(KRaft) + go_app + Nginx；打开 `http://localhost/` 注册即用。数据在命名卷（`mysql_data`/`app_upload`），`docker-compose down -v` 整体重置。
- 手动模式（宿主机 `go run .` 调试用）：自备 MySQL/Redis/Kafka，或复用 compose 里的——中间件端口只绑 127.0.0.1 且 MySQL 映射 **13306**（避开宿主机原生 3306 实例共存），宿主机直跑用 env 注入：`REDIS_ADDR=127.0.0.1:6379 KAFKA_BROKER=127.0.0.1:9092 MYSQL_DNS="root:…@tcp(127.0.0.1:13306)/ginchat?…"`。
- 配置优先级：**环境变量 > `config/app.yml`**（viper AutomaticEnv：`mysql.dns`→`MYSQL_DNS`、`redis.addr`→`REDIS_ADDR`、`kafka.broker`→`KAFKA_BROKER`、`port.server`→`PORT_SERVER`、`upload.path`→`UPLOAD_PATH`）。compose 靠 env 指向容器服务名，宿主机直跑不用改文件。
- 注意：`host.docker.internal` 只在容器内可靠指向宿主机；宿主机直连失败（`no such host`/握手被掐）时改用 env `REDIS_ADDR=127.0.0.1:6379` 等，别改提交文件。
- 多实例部署：为每个实例配置不同的 `snowflake.workerId`（0~31）

### 运行

```bash
docker-compose up -d --build    # 一键全栈，入口 http://localhost/
go run .                        # 宿主机直跑（自备中间件），监听 0.0.0.0:8081
go run test/bench_ws.go -mode echo -clients 5000 -wind 4   # WS 压测 v3（echo=闭环自回显 / conn=长连接承载；前置 dev 开关用 env 注入，见文件头）
```

浏览器访问 `http://localhost/`（compose）或 `http://localhost:8081/` 注册登录。

compose 需要 `.env`（新 clone 没有会直接起不来）：先 `copy .env.example .env`（Linux/macOS 用 `cp`），按需改密码。

## 架构总览

```
浏览器 ──HTTP──▶ Nginx ─静态/asset──▶ 本地目录
   │                 └─反代──▶ go_app(Gin) ──▶ service(HTTP控制器) ──▶ models(领域+IM引擎)
   └──WS /chat───────────────▲                 router/app.go 统一注册
                             │
              models/hub.go   16 分片 Hub（userId%16，每分片独立 goroutine 事件循环，无锁）
              models/message.go  recvProc→dispatch→sendMsg 两阶段投递
              models/store.go   会话层：convId(p_/g_) + seq 发号 + 冷热双写 + 未读数/Type4 事件
              utils/timewheel.go 心跳超时 O(1) 踢出 / 缓存延时双删
              utils/kafka.go    异步生产 + 消费组落存储（启动时 ensureTopic 幂等建 topic）
```

### 消息链路（两阶段投递）

1. **内存分发**：`Hub.GetHub(目标) → DataQueue(1024) → sendProc 写 WS`，队列满则背压踢线；
2. **异步落盘**：投 Kafka `chat_messages` → 消费者 `persistMessage`：热层 `msg_{小id}_{大id}` ZSet（修剪至 `storage.hotKeep` 条 + `hotTTLDays` 天滑动 TTL）+ 冷层批量写 MySQL `message` 表。

心跳是业务层 JSON（`Type:3`，前端每 10s 一发），只续期时间轮任务与会话 token、不转发；系统欢迎语走 `deliverLocal` 不进持久化。

### 鉴权

登录（`/user/findUserByNameAndPwd`）由服务端签发 32-hex 随机 **会话 token** 存 Redis（`login:token:{t}` = userId，TTL `auth.tokenTTLHours` 缺省 24h），并回写 `user.identity` 供前端携带。`/chat` 在 WebSocket 升级**之前**校验 token，未授权直接 401；心跳帧顺带滑动续期；修改资料/密码会撤销该用户全部会话（强制重登）。**全部业务 HTTP 接口走同一 `X-Token` 中间件**：身份从会话解析（表单 userId 不再可信），读会话的接口再做 `ConvHasMember` 成员校验——登了录也读不到别人的对话。单机架构下未选 JWT——长连接场景需要服务端可控的撤销/踢线能力，而这 JWT 反而要再引黑名单存储。

### 消息 ID 与会话 seq（双轨模型）

- **convId 会话命名空间**：私聊 `p_{小id}_{大id}`、群聊 `g_{群id}`（唯一来源 `convIdOf`，前端 `foot.html` 有镜像实现）。群成员共享 `g_{群id}`，任何成员都能回读全群历史；也彻底杜绝了旧 `msg_` 体系里用户 id 与群 id 串号的问题。
- **seq 会话级递增**（Redis `INCR seq:{convId}`，热层 ZSet 的 score）：排序/游标/未读计数全用它——小整数、支持"从第 N 条起增量拉取"、无时钟依赖。
- **msgId 雪花**（冷层 MySQL 主键）：全局唯一标识，位结构见下节。`(conv_key, seq)` 复合唯一索引保证发号完整性；Kafka 重放的真正投递幂等待后续用 clientMsgId 解决。
- **未读与已读**：HASH `unread:{uid}` 按会话计数，落库时给接收方（群聊为除发送者外全员）+1 并向在线端推 `Type=4` 事件；前端列表渲染红点，打开会话调 `/user/markRead` 清零并记 seq 游标（`user_read_cursor` 表落库，Redis `read:` 为快侧）。**私聊已读回执**：markRead 回填对端消息 `read_time` 并向发送方所有在线设备推 `Type=5`，对方气泡实时显示"已读"；群聊按成员水位记录（逐条全员回执留后续）。
- **多端在线**：Hub 按 `uid → []设备节点` 维护（`deviceId` 前端 sessionStorage 生成），一次投递扇出全部设备，背压/心跳只踢单端；同设备重连自动替换旧节点。浏览器双开标签页即可模拟多端。
- **幂等去重**：客户端为每条消息生成 `clientMsgId`，消费者据此在 Redis 近窗去重——Kafka 重放或用户误点重发不会再落第二条。

### 消息读取接口

| 接口 | 存储层 | 语义 |
|---|---|---|
| `POST /user/redisMsg` | Redis 热层 | `convId`（兼容 userIdA/B 推导），`start/end` 下标分页（历史契约：列表在响应 `Total` 字段） |
| `POST /user/historyMsg` | MySQL 冷层 | `convId + beforeSeq`（游标，0=最新）+ `size` 翻页，列表在 `Rows`（含 Seq） |
| `POST /user/unreadList` | Redis | 全部会话未读数 `{convId: count}` |
| `POST /user/markRead` | Redis+MySQL | 清会话未读 + 已读游标落库（私聊回填 read_time 并发 Type=5 回执） |
| `POST /user/readStatus` | Redis/MySQL | 会话对端已读游标（气泡"已读"状态来源） |

## 消息 ID：53-bit 雪花算法

`utils/snowflake.go`，一个 ID 就是一个 int64，但刻意只用 **53 bit**——同时满足两个下游约束：JS `Number.MAX_SAFE_INTEGER = 2^53-1`（前端不丢精度）和 Redis ZSet score 的 float64 尾数（不碰撞）。

```
 63 ............. 12 11 .... 7 6 ...... 0
┌──────────────────┬─────────┬──────────┐
│   41 bit 时间戳   │ 5 bit机器│ 7 bit序列 │   （高 10 bit 恒 0，凑 53 bit 上限）
└──────────────────┴─────────┴──────────┘
```

- **纪元**：自定义 `epoch = 2024-01-01 00:00:00 UTC`，41 bit 毫秒可用约 69 年（至 2093）
- **机器码 5 bit**：最多 32 节点；**序列 7 bit**：单机每毫秒最多 128 个，用完自旋等下一毫秒
- **配置**：`main.go` 启动时以 `snowflake.workerId`（0~31）显式初始化，**多实例部署必须逐实例配不同值**——撞 ID 会表现为冷存主键冲突跳过（cold writer 有【警告】日志）
- **落点**：冷层 MySQL 主键 `id` 显式写入此 ID（全局唯一标识）；**会话排序/游标改用 seq 双轨**（见上节），热层 score 不再是雪花
- **约束**：任何一段位数都不得拓宽，否则前端精度丢失 / score 碰撞

## 目录结构

```
main.go        启动编排（顺序有依赖，见文件内注释）
router/        路由注册            service/   HTTP 控制器（Swagger 注解在此层）
models/        领域模型 + IM 引擎：hub.go 分片连接中心、message.go 消息生命周期、
               store.go 冷热存储、contact.go 好友/群关系、userBasic.go 用户缓存、
               community.go 群、migrate.go 建表
utils/         基础设施：system_init(MySQL/Redis)、kafka、timewheel 时间轮、
               snowflake、md5、resp 统一响应
views/ asset/  服务端模板与静态资源（上传文件落 asset/upload/，已 gitignore）
config/        app.yml             sql/  init_ginchat.sql
test/          bench_ws.go 压测     docs/  swag 生成物
```

## API 一览

`GET /` `/index` `/toRegister` `/toChat` `/chat`(WS) `POST /user/{createUser,findUserByNameAndPwd,updateUser,getUserList,redisMsg,historyMsg}` `DELETE /user/deleteUser` `POST /searchFriends /contact/{addfriend,deletefriend,createCommunity,loadCommunity,joinGroup} /attach/upload`；pprof：`/debug/pprof/*`

## 性能与压测

压测入口（v3，阶段化）：`go run test/bench_ws.go -mode echo|conn|ack ...`。服务端前置 dev 开关用 env 注入（勿写回提交的 yml）：`AUTH_DEVSKIPWSTOKEN=true go run .`——bench 启动前先试探拨一条连接，401/拒连直接报缺哪个开关。**管线翻转后投递挂在落盘之后**：echo/ack 档要求 `STORAGE_SKIPBENCHPERSIST=false`（缺省即 false；开旁路则两档收不到任何帧，只有 conn 档可跑），压测数据落库、跑完按 `client_msg_id LIKE 'bench-%'` 清理；跑前确认 Kafka LAG 近零（历史积压会淹没新消息）。参数：`-start` uid 号段基（真实用户同时在线时调大避串扰，如 1000000）、`-hb` 心跳间隔（默认 10s，须小于服务端 `timeout.DelayHeartbeat`）、`-timeout` 单条往返超时（超时判丢包）。

**三种模式（对齐野火 IM 阶段化思路：连接承载 / 分发+通知 / 发送→落库→回执）**

| 模式 | 测什么 | 覆盖路径 |
|---|---|---|
| `-mode conn` | 长连接承载：拨号时延直方图、掉线归因(read/write)、单连接成本（RSS 增量/连接数；预期 35~50KB——DataQueue 1024 槽 chan + gorilla 读写缓冲 + 每连接 2 goroutine）。判据：持续 ≥2×DelayHeartbeat(120s) 在线不衰减 | 连接层：Upgrade → register → 时间轮心跳保活 |
| `-mode echo` | 闭环回显（`TargetId=self`，配平点=落盘后分发的 own 帧=接收方视角全往返）：`-wind` 为在途窗口（1=一发一收），**QPS 是测量结果而非参数乘积**；超时未回按 `-timeout` 判丢、进丢包率不进时延分位；接收帧按 clientMsgId 严格配平，匹配/dup/非业务三桶（Type=4/5 事件与欢迎帧只进 unexpected，不再灌"接收 QPS"） | 分发+通知段（落盘后投递全往返） |
| `-mode ack` | 全链路闭环：**配平点是服务端 Type=6 ACK**（冷层写成功+放大分发后才回），RTT=发送→Kafka→落库→分发→回执；服务端拒收(Status=-1)计入丢包并单列"拒收"。必须 `STORAGE_SKIPBENCHPERSIST=false`（旁路档永不回 ACK），数据落库按 `client_msg_id LIKE 'bench-%'` 清理 | 野火"发送阶段"端到端（落库+分发+ACK） |

**v2.1 → v3 的教训（v2.1 的"几万"不可信）**：开环定频发送——发送 QPS ≡ clients×rate，服务端再死数字照样输出几万；丢包零记账——背压 `dropNode` 静默吃消息无处体现；时延直方图只算收到的消息（幸存者偏差，丢得越狠 P99 越好看）；关 skipBenchPersist 全链路档时 Type=4 事件帧使"接收 QPS"翻倍。v2 的修正（Type=3 心跳同协程交替写、补 UserId、断连即减在线）在 v3 全部保留，另外新增：401 试探拨、指数退避自动重连（保持 N 恒定，dropped 计数区分被踢/自杀）、在途 gauge（持续增长=服务端消化不动的饱和信号）。

**容量判读 SLO**：丢包率 < 0.1% 且 RTT P99 < 100ms 的档位下，最大"完成 TPS"才算容量。预期回显会回落到 1w+ 量级——那才是诚实数字。推荐矩阵：`clients ∈ {500,2k,5k,10k} × wind ∈ {1,4,16}`，每档 `-duration 180` 取中段。Windows 单机对同一目标拨号受动态端口池（约 16k）上限，>1.5w 连接从 WSL/Linux 打流，否则失败是打流端端口耗尽。客户端自报"在线"必须对真值：`netstat` :8081 ESTABLISHED、`/debug/pprof/goroutine`、服务进程 RSS。

**仍未覆盖（出全量报告前须知）**：① ~~落库闭环~~ **已由 `-mode ack` 覆盖**——Type=6 ACK 即"落库+分发完成"凭证，ack 档测的才是野火"发送阶段"端到端（单分区+单消费者+每条 4~6 次同步 Redis 往返仍是全链路分母，ack 档 TPS 明显低于 echo 档属预期而非退步）；② ring 跨分片分发与群聊 1→N 扇出（成本放大器：1 条 = N 设备扇出 + N 未读 HINCR + N 事件 + N ACK；需先批量造社交图谱 addfriend/createCommunity/joinGroup 再按 7:2:1 混流）；③ 读接口（redisMsg/historyMsg/unreadList）零流量；④ 慢消费者单端被踢专项；⑤ 多机打流；⑥ 同机回环时延偏乐观，P99 不含真实网络 RTT/抖动；⑦ 真实鉴权负载（bench 用 token="test"，TouchToken 对不存在 key 的 Redis EXPIRE 属 dev 噪声）。

**分层指标（SLO 建议基线）**

| 层 | 指标 | 采集方式 | 基线 |
|---|---|---|---|
| 连接层 | 建连速率/耗时、并发连接数 | `-mode conn` 拨号时延直方图 + monitor（对 netstat 真值） | 2万连接 < 30s，失败率 0，120s 内在线不衰减 |
| 消息层 | **往返 P50/P99 + 丢包率**（闭环配对，超时判丢不进分位） | `-mode echo` 内置往返直方图 | 稳定期 P99 < 100ms，丢包 < 0.1% |
| 消息层 | 完成 TPS、在途积压趋势 | monitor（完成/s + 在途 gauge） | 加档时 TPS 线性升、在途不持续增长 |
| 消息层 | 各分片连接均衡度 | pprof 或加只读统计（`ClientCount` 为全量计数，分片计数需遍历 shards） | 分片偏差 < 2× |
| 全链路 | **发送→落库→ACK P99 与落库 TPS** | `-mode ack`（skip=false 档；拒收单列） | P99 < 2s，完成 TPS=落库 TPS，丢包<0.1% |
| 持久层 | Kafka consumer lag | `kafka-consumer-groups.sh --describe` | 稳定期不增长（**大 LAG 会淹没新消息**，ack 档要求近零） |
| 进程层 | goroutine / GC / 内存 / 单连接成本 | `/debug/pprof/*` 压测中 30s 采样；RSS增量/clients | goroutine ≈ 连接数×2+常数；RSS/连接 ≤ 50KB |

**实战踩坑记录**：① Kafka 容器被 SIGKILL（Docker Desktop 重启/内存压力）后，`chat_messages` 日志可能回退而 `__consumer_offsets` 存活——**LAG 显示负数、committed > log-end，消费组从此永远拉不到新消息**。排查：`kafka-consumer-groups.sh --describe` 看 LAG 符号；恢复（练手库）：`kafka-topics.sh --delete --topic chat_messages` 让服务启动时 ensureTopic 重建即可。② **管线翻转后的新坑：生产者改同步后错误可见了，但 LAG 大时新消息的投递与 ACK 会被积压淹没**（实测：v2.1 时代留下的 410 万条 bench 残留把 ACK 全部顶成超时）——ack 档压测前必须确认 LAG 近零；skip 档旁路残留靠 `-mode ack` 无法消化，要么开着 `STORAGE_SKIPBENCHPERSIST=true` 让它空跑提交，要么删 topic 重建。

**故障与恢复场景**：① 重连风暴（杀服务端→bench v3 全端自动指数退避重连，专项观察：登录 Redis QPS、消费者 rebalance、时间轮任务堆积；dropped 计数区分被踢/自杀；前端 pending 队列在 onopen 全量重发，服务端幂等门禁保证不落第二条、只补 ACK）；② **断 Kafka（语义已反转：先落盘再投递——同步 produce 失败进应用层重投队列（2s×5 轮），超限回 Type=6 Status=-1，前端气泡变红可点重发；不再有"内存照常送达"的假可用，这是可靠性的代价）**；③ 断 MySQL（冷写重试耗尽 → 清去重标记 + 原文进 `msg:dlq`（封顶500）+ 错误 ACK-1，offset 照常提交防毒消息占分区；重连后可按死信人工补偿——注意热层已写入的 seq 已消耗，同 clientMsgId 重发成功会再占一个 seq，热层同原文覆盖不重复）；④ 优雅停机（SIGINT 后 `coldCh` 排空=落完+投完+ACK 完，看 "final flush+deliver done"）；⑤ 参数非法（即时 ACK-1，不入库不重投，与存储故障语义分开）。

**容量估算**：内存 ≈ 连接数×(读写缓冲 8KB×2 + DataQueue 最坏 1024×消息均值) + 热层 Σ(会话数×hotKeep×消息大小)；注意 workerId 上限 32 实例、seq 当前为消费者单点 INCR——这两条是水平扩展前的硬边界。单机 2 万连接已近 Docker/客户端自身 fd 上限时，把 client 拆到多机压。

**回归护栏**：每次性能相关改动跑 30min 稳态对比基线 P99；`【警告】`（冷存冲突/收尾超时）必须为 0 条。

## 数据治理与回归验证

### 冷层瘦身 runbook（`message` 只写不删，练手栈 MySQL 卷会无声膨胀）

```bash
# ① 看规模（行数 + 磁盘占用）
docker exec webapp-mysql-1 sh -c 'mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -N -e "SELECT COUNT(*) FROM ginchat.message; SELECT ROUND(SUM(data_length+index_length)/1073741824,2) FROM information_schema.tables WHERE table_schema=\"ginchat\" AND table_name=\"message\";"'
# ② 只清冷归档（保用户/群/关系；seq 从 1 重计无害——热层键与门禁会随 TTL/修剪自然收敛）
docker exec webapp-mysql-1 sh -c 'mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -e "TRUNCATE TABLE ginchat.message;"'
# ③ 彻底重来（连卷带容器全拆，重启服务 AutoMigrate 重建表）
docker-compose down -v
```

压测残留可先精准清：`DELETE FROM ginchat.message WHERE client_msg_id LIKE 'bench-%';`。TRUNCATE 与门禁/游标相互独立——清完最多加一条 `docker exec webapp-redis-1 redis-cli --scan --pattern 'cm:*' | xargs -r docker exec webapp-redis-1 redis-cli DEL` 的可选 tidy。知道刹车在哪，才敢一直踩油门。

### 管线回归验证工装（改管线必跑）

- **端到端**：`test/verify/`（常驻入库）——S1 私聊聚合、S2 小群扇出、S3 大群 Type=7+计算式未读、S4 多端扇出+心跳隔离踢线、S5/S6 clientMsgId 幂等与重放风暴。前置 env 与服务端同一组（见 `test/verify/main.go` 文件头），`go run ./test/verify`（`-clean` 清场；退出码非 0 = 有 FAIL，冲突取证看服务端日志「冲突样本」行）。
- **纯逻辑单测**：`go test ./utils ./models ./middleware`——雪花唯一性/溢出换纪元/位段纯净、时间轮触发/续期/取消、convIdOf 归一化、落库前幂等对账 split、心跳键按设备隔离。

## 已知局限与后续规划

待做（按优先级）：

- 水平扩展：跨节点消息路由（uid→节点路由表）+ seq 发号去单点假设；完成后才可多实例部署（workerId 上限 32）。落盘后置分发使**单分区消费者成为全链路吞吐上限**，扩分区/多消费者组应与此项一并规划
- 离线推送厂商实现（接口与挂点已留，缺 APNs/FCM/短信等凭证与通道）
- 消息撤回/编辑
- 冷存归档分区、Redis 内存治理（全局用量巡检，当前仅单会话修剪）
- 口令哈希升级为 bcrypt/argon2；日志结构化（zap）；补单元测试（convIdOf/seq/时间轮/persistMessage 幂等分支都是纯逻辑，好测）

## License

未指定（私有仓库）。
