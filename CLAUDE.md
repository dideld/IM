# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概况

Go 单体 Web IM（即时通讯）系统：Gin + gorilla/websocket 长连接，MySQL（GORM）持久化，Redis 缓存/离线消息，Kafka 异步落盘。前端为服务端渲染 HTML（`views/` + `asset/`）。Go 1.25。

## 常用命令

```bash
go build .                      # 编译
go run .                        # 本地运行，监听 0.0.0.0:8081
docker-compose up --build       # 部署：nginx(:80，托管 /asset 并反代 WS) + go_app(:8081)
go run test/bench_ws.go -mode echo -clients 5000 -wind 4   # WS 压测 v3（conn=长连接承载/echo=闭环自回显；前置 dev 开关 env 注入见文件头，勿在常驻配置里留着）
```

- 本仓库没有 Go 单元测试（无 `_test.go`）。`test/` 目录下是 `package main` 的独立工具程序，用 `go run` 跑，不要用 `go test`。
- pprof 已注册：`/debug/pprof/*`。Swagger 注解写在 `service/` 各处理函数上，`docs/` 为 swag 生成物（改注解后用 `swag init` 重新生成）。

## 运行前提与配置坑

- 依赖 MySQL / Redis / Kafka。地址默认值在 `config/app.yml`（含 `kafka.*`/`storage.*`/`auth.*`/`upload.*`）；**viper AutomaticEnv 支持环境变量覆盖**（`mysql.dns`→`MYSQL_DNS` 等，env 优先于文件）——宿主机直跑连 127.0.0.1 用 env 注入，**不要为临时测试改提交 yml**。
- **一键全栈**：`docker-compose up -d --build` 起 MySQL+Redis+Kafka+go_app+Nginx（compose 里 go_app 用 env 指向容器服务名；中间件端口只绑 127.0.0.1，MySQL 对外 13306 避开宿主机原生实例；Kafka 双监听器 INTERNAL `kafka:9092` / HOST `localhost:9092`）。依赖镜像拉取需宿主机代理在线（Docker 配了 127.0.0.1:7890）。
- 上传目录走 `upload.path`（缺省 `./asset/upload`，不存在自动创建），单文件上限 `upload.maxSizeMB`（缺省 10MB，`MaxBytesReader` 硬限）；扩展名白名单在 `service/attach.go`（图片+音频，白名单外拒绝——该目录被 Nginx 直接托管）。`asset/upload/` 与 `cache/` 已 gitignore，用户产物不得再提交。
- **宿主机直跑注意**：`host.docker.internal` 是给容器内访问宿主机用的；宿主机自己解析它依赖 Docker Desktop 运行或 `C:\Windows\System32\drivers\etc\hosts` 的条目（该条目可能被注释）。解析失效时 `go run .` 表现为 `no such host` 或 TCP 通但握手被掐——临时把三个地址改 `127.0.0.1` 测试，**提交里保持 `host.docker.internal`**（docker 部署需要）。
- 初始化顺序有依赖（`main.go`）：InitConfig → InitMysql → models.Migrate（AutoMigrate 同步表结构）→ InitRedis → utils.InitSnowflake(`snowflake.workerId`) → models.StartColdWriter → InitKafka（生产者，内含 ensureTopic）→ StartKafkaConsumer（消费者）。时间轮 `utils.TW` 和 UDP 监听（3001 端口）由包 `init()` 自动启动，不在 main 里显式出现。
- **优雅停机**（SIGINT/SIGTERM）：HTTP `srv.Shutdown` → `utils.CloseKafka`（停消费循环、关读写端）→ `models.StopColdWriter`（排空冷队列、落完最后一批，同步等待最长 10s）。新增需要收尾的后台组件时，按此顺序挂接。
- **Kafka 故障排查**：容器被 SIGKILL（Docker Desktop 重启/OOM）后可能出现 LAG 为负、committed > log-end 的脏状态——消费组永远拉不到新消息，且 Async 生产者吞错（服务端看似正常，只有"落库为零"这一症状）。诊断：`kafka-consumer-groups.sh --describe`；练手恢复：删 `chat_messages` topic，启动时 ensureTopic 重建即可。

## 架构与核心链路

### 分层

```
main.go → router/app.go（路由注册）→ service/*（HTTP controller，Swagger 注解在此层）
        → models/*（领域模型 + IM 引擎）→ utils/*（基础设施初始化、时间轮、雪花 ID、Kafka）
```

### 消息主链路（核心，跨 message.go / hub.go / kafka.go / timewheel.go）

1. 客户端连 `/chat?userId=x&token=y`（唯一 WS 入口；token 为登录签发的 Redis 会话，见"鉴权会话"）→ 创建 `Node`，注册进分片 Hub，起 `sendProc` / `recvProc` 两个 goroutine。
2. `recvProc` 收到帧后解析 JSON：`Type==3` 是心跳（业务层 JSON 消息，不是 WS ping 帧），只 `TouchToken` 续会话 + 刷新时间轮任务并 `continue`；其余交给 `dispatch`（Type 1=私聊，2=群聊，群聊查 contact 表拿成员逐个下发）。
3. `sendMsg` 两阶段投递：
   - **阶段一（内存，低延迟）**：`WsHub.GetHub(目标)` → `send` channel → 目标 `DataQueue` → `sendProc` 写 WS。
   - **阶段二（异步，可靠性）**：投 Kafka topic `chat_messages`（Async 生产者）→ 消费者组消费 → `models.persistMessage` 冷热双层落存储（见"消息冷热存储"）。系统提示语（如欢迎语）走 `deliverLocal`（仅内存分发，**不进 Kafka/持久化**，历史上曾以纯文本污染 `msg_0_0` key）。
   - 两阶段顺序不可调换、不可省略阶段二：注释标明设计意图是"先瞬间送达，再异步落盘，进程崩溃不丢消息"。

### 鉴权会话（`utils/auth.go`，P0 新增）

- 登录（`models.FindUserByNameAndPwd`）签发 32-hex `crypto/rand` token：`login:token:{t}` = userId（TTL `auth.tokenTTLHours`，缺省 24h），并登记到 `login:user:{uid}` 集合；token 回写 `user_basic.identity`，前端 `/toChat?token=` 一路带到 `/chat`。
- `/chat` 握手**在 Upgrade 之前**校验 `utils.ValidateToken(token, userId)`，失败回 401 不升级连接；心跳帧（Type=3）调 `TouchToken` 滑动续期（token 与会话集合一起）。
- `models.UpdateUser` 调 `DropUserSessions` 撤销该用户全部会话（强制重新登录；已建立的 WS 不受影响，只挡新连接）。
- **HTTP 层鉴权（P1.5）**：`service.AuthRequired` 中间件保护除登录注册、`/chat`（自校验）、页面/静态/Swagger 外的全部业务接口；前端 `head.html` 的 `post/uploadfile/uploadblob` 与 `util.js` 的 `util.post` 统一带 `X-Token` 头（值取页面 URL 的 token），401 时 toast 并跳登录。**各 handler 一律以 `authUid(c)` 为身份来源，绝不再信表单里的 userId/ownerId**；读会话接口还要过 `models.ConvHasMember(convId, uid)` 成员校验（合法会话也读不了别人的对话）。
- **改密链路**：`updateUser` 密码非空时先 `MakePassword` 加盐哈希（沿用旧盐，无盐轮换）再落库，`models.UpdateUser` 的 Updates map 仅在 `PassWord`/`Salt` 非空时写入——明文永不进 DB；变更后 `DropUserSessions` 全端踢线。
- Redis key 命名空间：`login:*`（鉴权）、`user_info:*`（缓存）、`p_*`/`g_*`（热层会话，见"Redis key 约定"）互不冲突，新增鉴权相关 key 请保持 `login:` 前缀。

### 分片 Hub 与 Node 生命周期（`models/hub.go`，改动前必读）

- 多端在线（P2）：`clients map[int64][]*Node`（uid → 各设备节点），`Node.DeviceId` 来自 `/chat?deviceId=`（前端 sessionStorage 每标签页一个，双开标签页即可测多端）。register 时**同 DeviceId 替换旧节点**（页面刷新重连场景）；`send` 扇出到该用户全部设备；`dropNode` 按**指针身份**只摘自己——背压/心跳只踢单台设备，不影响其余端，也防止误删同设备重建的新连接（同 uid 多端后更不能按 uid 整删）。
- `HubManager` 固定 16 分片，`userId % 16` 路由；每个 Hub 一个独立 goroutine 事件循环（`Run`），通过 register/unregister/send channel 通信，**无锁**——`clients` map 只允许在所属分片的 `Run` 内触碰，禁止在别的 goroutine 直接读写。
- `Node` 持有 `CurrentHub`（所属分片）和 `DataQueue`（缓冲 1024）；队列满 `dropNode` 单端（close DataQueue 后 sendProc 关该端 TCP，客户端感知被踢）。
- `/chat` 握手有 dev 开关 `auth.devSkipWSToken`（跳过 token 校验，**仅供 bench_ws 压测，生产严禁开启**；yml 不内置该项，压测时用 env `AUTH_DEVSKIPWSTOKEN=true` 注入，bench v3 启动会试探拨并在 401 时直接提示开关缺失）。

### 时间轮（`utils/timewheel.go`）

全局单例 `utils.TW`（1s × 60 槽，超过 60s 用圈数表达）。同 key 的 `AddTask` 等于 O(1) 续期，`RemoveTask` O(1) 取消。当前两类任务：
- 心跳超时踢出：key = `userId`（int64），连接注册和每次心跳都续期。
- 缓存延时双删：key = `"double_delete_"+cacheKey`（string）。新增任务 key 时注意与这两类命名空间不冲突。

### Redis key 约定（`utils/system_init.go` / `models`）

- `user_info:{id}`：用户缓存。读路径为 Cache-Aside + `"null"` 占位 60s 防穿透 + `singleflight` 防击穿（见 `FindUserById`）；写路径为延时双删——先删缓存 → 改库 → 时间轮挂 500ms 后第二删 + 异步重建（见 `UpdateUser`）。改用户缓存逻辑须保持这套组合完整。
- **会话 id（convId，P1 引入）**：私聊 `p_{小id}_{大id}`、群聊 `g_{群id}`，唯一来源 `models/store.go` 的 `convIdOf`——**`views/chat/foot.html` 有它的 JS 镜像实现 `convIdOf`，改 Go 侧必须同步改前端**。热层 ZSet key、冷层 `conv_key` 列、未读 field、Type=4 事件全用它；`msg_` 前缀旧体系已一刀切废弃。
- `seq:{convId}`：会话级递增序号发号器（`INCR`，消费者是唯一写入点）。**热层 ZSet 的 score = seq**（不再是雪花；53-bit 精度约束现在只剩 msgId 需要）。ZSet 修剪至 `storage.hotKeep` + 滑动 TTL 逻辑不变。
- **消息冷热存储（`models/store.go`）**：消费者回调 `persistMessage`：clientMsgId 去重（`cm:{convId}` SET 近窗判重，超 2×hotKeep 随机淘汰，远期靠主键冲突兜底）→ 发 seq → 热层 Pipeline → 投 `coldCh` 由 `StartColdWriter` 批量 `CreateInBatches`（主键 id=雪花 msgId，`(conv_key, seq)` 复合唯一索引 `uk_conv_seq`；OnConflict DoNothing + 冲突跳过计数【警告】；停机由 `StopColdWriter` 同步排空）。
- **未读、已读与回执**：HASH `unread:{uid}`（field=convId）由 `pushUnread` 累加（私聊给接收方；群聊给除发送者外全体成员），向在线端推 `Type=4` 事件 `{"Type":4,"ConvId","Seq","Unread"}`。`MarkRead`：清未读 + Redis 游标 + **`user_read_cursor` 表 upsert 落库**（markRead 频率低，同步写可接受，上量再改批量 channel）；**私聊**回填对端消息 `read_time` 并向对方在线设备推 `Type=5` 回执 `{"Type":5,"ConvId","ReadSeq","Reader"}`；群聊只记成员水位（逐条全员回执留后续）。前端 `readStatus` 接口 + `convReadSeq` 驱动气泡"已读"。事件帧 **仅服务端→客户端**，前端 `onmessage` 必须在 `loaduserinfo` 之前拦截 Type=4/5（否则无效 HTTP + 污染 msglist）。
- 读接口四条（全部经 X-Token 会话鉴权 + `ConvHasMember` 成员校验）：`/user/redisMsg` 热层（`convId` 参数，兼容 userIdA/B 推导；**响应列表在 `Total` 字段的前端契约勿改**）；`/user/historyMsg` 冷层（`convId + beforeSeq + size`，Rows 含 Seq，末行 seq 翻下页）；`/user/markRead`、`/user/unreadList`、`/user/readStatus`（身份取会话，表单 userId 被忽略）。

### 雪花 ID（`utils/snowflake.go`）

刻意做成 **53 bit**（41 时间戳 + 5 机器码 + 7 序列），以同时兼容 JS `Number.MAX_SAFE_INTEGER` 和 Redis ZSet 的 float64 score。**不要拓宽任何一段位数的占比**，否则前端精度丢失、ZSet score 碰撞。

- `main.go` 显式 `InitSnowflake(viper.GetInt64("snowflake.workerId"))`（0~31）。**多实例部署必须逐实例配置不同 workerId**，否则同毫秒撞 ID → 冷存主键冲突被跳过（cold writer 的【警告】日志就是抓这个的）。seq 双轨：排序/游标/未读用 seq，msgId（雪花）是冷层主键与全局唯一标识；投递幂等由 clientMsgId（P2）承担。

### 数据模型约定

- 所有表用 `gorm.Model`（软删除 `deleted_at`），启动时 `models.Migrate()` AutoMigrate 同步结构，建表脚本 `sql/init_ginchat.sql` 是快照（其中 message 段含 DROP 重建，见下）。
- `message` 表为冷存储全量归档：主键 id 显式写入雪花 ID（非自增），另有 `seq`（会话内递增号）与 `conv_key`，`(conv_key, seq)` 复合唯一。旧 DDL（form_id/target_id longtext）与新结构不兼容，开发库需先 `DROP TABLE message;` 一次。
- `Contact.Type`：1=好友（**双向各存一行**，增删须成对处理，见 `AddFriend`/`DeleteFriend`），2=群成员关系（owner_id=用户，target_id=群）。
- `Message.Type`：1 私聊 / 2 群聊 / 3 心跳 / 4 未读事件 / 5 已读回执（**4、5 仅服务端→客户端**，客户端若回发会走 dispatch 无匹配分支被无害忽略）；`Media`：文字、表情、图片、音频；`Amount`（原拼写错误字段 `Amoubt` 已改名，改后前端 `amount` JSON 才真正绑定得上）；`ClientMsgId`：客户端幂等键（`genMsgId()` 生成）。
- `user_read_cursor` 表：`(user_id, conv_key)` 唯一，`last_seq` 为已读水位——Redis `read:` 游标的持久账本。

### 已清理的遗留路径

旧 Redis pub/sub WS 链路（`/user/sendMsg`、`utils.Publish/Subscribe`、`PublishKey`）、`utils/tadk_init.go`、`asset/md5.go`（曾随 Nginx 静态目录公开）、`test/testGorm.go`、`views/user/index.html/test.html`、`Node` 上的遗留心跳方法均已删除。`/chat` 是唯一 WS 入口。

## 编码约定

- 注释与 commit message 均为中文，跟随现有风格。
- HTTP JSON 响应统一走 `utils.RespOk / RespFail / RespList / RespOkList`（code 0 成功、-1 失败），不要手写 `c.JSON` 新格式。
- HTML 模板经 `r.LoadHTMLGlob("views/**/*")` 加载，`c.HTML` 引用名为 `"login.html"`、`"chat/index.html"` 这类（以文件名或相对 views 的路径为 template name）。
