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
- **未读与已读**：HASH `unread:{uid}` 按会话计数，落库时给接收方（群聊为除发送者外全员）+1 并向在线端推 `Type=4` 事件；前端列表渲染红点，打开会话调 `/user/markRead` 清零并记录 seq 游标（`read:{uid}:{convId}`，多端同步预留）。

### 消息读取接口

| 接口 | 存储层 | 语义 |
|---|---|---|
| `POST /user/redisMsg` | Redis 热层 | `convId`（兼容 userIdA/B 推导），`start/end` 下标分页（历史契约：列表在响应 `Total` 字段） |
| `POST /user/historyMsg` | MySQL 冷层 | `convId + beforeSeq`（游标，0=最新）+ `size` 翻页，列表在 `Rows`（含 Seq） |
| `POST /user/unreadList` | Redis | 全部会话未读数 `{convId: count}` |
| `POST /user/markRead` | Redis | 清会话未读 + 写 seq 已读游标 |

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

## 已知局限与后续规划

已完成（P0）：WS 鉴权（Redis 会话 token）、雪花 workerId 配置化 + 主键冲突告警、优雅停机（冷队列排空）、上传白名单/大小校验。
已完成（P1）：会话实体化（convId `p_`/`g_` 命名空间）、seq 双轨（排序/游标/未读）、群历史全员统一回读、未读红点+打开即已读+在线 Type=4 事件。

已完成（P1.5）：HTTP 层鉴权（X-Token 会话身份 + ConvHasMember 成员校验，表单 userId 不再可信）、改密加盐哈希落库 + 全端踢线、401 统一跳登录、群历史下拉翻页。

待做（按优先级）：

- 消息幂等去重（clientMsgId，解决 Kafka 至少一次的重放复制）、多端在线（Hub 按设备分叉）
- 已读回执与 `Message.ReadTime` 落库
- 水平扩展（uid→节点路由表 + 跨节点消息转发；注意 seq 发号当前假设消费者单点）、冷存归档分区、Redis 内存治理
- 口令哈希仍为 MD5+盐（建议 bcrypt）；日志为裸 fmt.Println、无单元测试

## License

未指定（私有仓库）。
