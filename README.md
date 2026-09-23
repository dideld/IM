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

- Go 1.25；MySQL 8（执行 `sql/init_ginchat.sql` 初始化，`message` 冷存表也可留空由启动时 AutoMigrate 生成）
- Redis、Kafka（单机即可；地址全部配置在 `config/app.yml`，代码含默认值回退）
- 注意：`host.docker.internal` 仅在 Docker 容器内可靠指向宿主机；宿主机直接 `go run .` 时若连不上（`no such host` 或握手被断），把配置里三个地址改为 `127.0.0.1`，或取消 `C:\Windows\System32\drivers\etc\hosts` 中对应注释行
- 多实例部署：为每个实例配置不同的 `snowflake.workerId`（0~31）

### 运行

```bash
go run .                        # 宿主机直跑，监听 0.0.0.0:8081
docker-compose up --build       # 部署栈：nginx(:80) → go_app(:8081)，/asset 由 Nginx 托管
go run test/bench_ws.go         # 2 万连接 WS 压测（独立 main 程序，仓库无单元测试）
```

浏览器访问 `http://localhost/`（compose）或 `http://localhost:8081/` 注册登录。

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

压测入口：`go run test/bench_ws.go`（2 万连接 × 每人每秒 8 条 ≈ 16 万消息级；前置 `auth.devSkipWSToken: true` 跳过 WS 鉴权，生产严禁）。

**分层指标（SLO 建议基线）**

| 层 | 指标 | 采集方式 | 基线 |
|---|---|---|---|
| 连接层 | 建连速率/耗时、并发连接数 | bench 日志 | 2万连接 < 30s，失败率 0 |
| 消息层 | **端到端延迟 P50/P99**（发送→接收） | 见下"延迟直方图" | P99 < 200ms |
| 消息层 | 服务端 QPS、各分片连接均衡度 | 内置 monitor + `ClientCount` 分片计数 | 分片偏差 < 2× |
| 持久层 | 落库滞后（发送→冷层可见） | Type=4 事件的 Seq 对时 或 kafka lag | < 5s |
| 持久层 | Kafka consumer lag | `kafka-consumer-groups.sh --describe` | 稳定期不增长 |
| 进程层 | goroutine / GC / 内存 | `/debug/pprof/*` 压测中 30s 采样 | goroutine ≈ 连接数×2+常数 |

**扩展 bench 的三个关键改造**（当前只测吞吐，不测时延）：
1. **延迟直方图**：发送时记 `clientMsgId + 本地时间`，接收端按 `clientMsgId` 配对算单跳时延，HDR 桶输出 P50/P99/P999——现有原子计数器只能算"总包数/秒"。
2. **流量模型贴近真实**：按 7:2:1 混入私聊:群聊:心跳，群规模 3~200 人——群消息是成本放大器（1 条 = 扇出 N 设备 + N 次未读 HINCR + N 个 Type=4 事件 + 1 次 Kafka + 1 次热层 + 冷队列入队），峰值 QPS 上限基本由群占比决定。
3. **慢消费者专项**：某连接故意不收（停 read），验证 Hub 背压**只踢该设备**、其同伴设备无恙（多端模型后新增的正确断言）。

**故障与恢复场景**：① 重连风暴（杀服务端→全端重连：盯登录 Redis QPS、消费者 rebalance、时间轮任务堆积）；② 断 Kafka（消息只走内存阶段、发送不报错，恢复后补落盘）；③ 断 MySQL（冷批失败只记日志，热层/投递不受影响——降级路径的验证）；④ 优雅停机（SIGINT 后 `coldCh` 应排空、0 丢消息，看 "final flush done"）。

**容量估算**：内存 ≈ 连接数×(读写缓冲 8KB×2 + DataQueue 最坏 1024×消息均值) + 热层 Σ(会话数×hotKeep×消息大小)；注意 workerId 上限 32 实例、seq 当前为消费者单点 INCR——这两条是水平扩展前的硬边界。单机 2 万连接已近 Docker/客户端自身 fd 上限时，把 client 拆到多机压。

**回归护栏**：每次性能相关改动跑 30min 稳态对比基线 P99；`【警告】`（冷存冲突/收尾超时）必须为 0 条。

## 已知局限与后续规划

已完成（P0）：WS 鉴权（Redis 会话 token）、雪花 workerId 配置化 + 主键冲突告警、优雅停机（冷队列排空）、上传白名单/大小校验。
已完成（P1）：会话实体化（convId `p_`/`g_` 命名空间）、seq 双轨（排序/游标/未读）、群历史全员统一回读、未读红点+打开即已读+在线 Type=4 事件。

已完成（P1.5）：HTTP 层鉴权（X-Token 会话身份 + ConvHasMember 成员校验，表单 userId 不再可信）、改密加盐哈希落库 + 全端踢线、401 统一跳登录、群历史下拉翻页。
已完成（P2）：clientMsgId 幂等去重（Kafka 重放/客户端重发不落第二条）、多端在线（uid→设备列表扇出、同设备重连替换、背压只踢单端）、已读回执（Type=5 事件 + 气泡"已读"）、read_time 私聊回填、user_read_cursor 游标落库、压测方法论（见上节）。

待做（按优先级）：

- 水平扩展：跨节点消息路由（uid→节点路由表）+ seq 发号去单点假设；完成后才可多实例部署（workerId 上限 32）
- 群聊逐条已读回执（当前只记成员水位）、消息撤回/编辑、离线推送
- 冷存归档分区、Redis 内存治理（全局用量巡检，当前仅单会话修剪）
- 口令哈希升级为 bcrypt/argon2；日志结构化（zap）；补单元测试（convIdOf/seq/去重/时间轮都是纯逻辑，好测）

## License

未指定（私有仓库）。
