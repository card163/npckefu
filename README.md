# npckefu — 智能客服 SaaS（Go 后端）

多租户智能客服后端：机器人优先接待 → 转人工 → 技能组/负载路由 → 排队。IM 模型参考云信 IM 2.0：
`appkey/appsecret` 租户、服务端注册 `accid`+`token`、客户端 WebSocket 登录、会话内递增 `seq`、
`client_msg_id` 幂等去重、`ack`、断线按 `since_seq` 同步。

## 运行
    docker compose up -d                          # 本地 MySQL 8（库 npckefu）
    DB_DSN='root:@tcp(127.0.0.1:3306)/npckefu' ADMIN_TOKEN=xxx go run ./cmd/server   # :8080，启动时自动建表
    # 可选 LLM（OpenAI 兼容）：LLM_API_KEY / LLM_BASE_URL / LLM_MODEL
    TEST_MYSQL_DSN='root:@tcp(127.0.0.1:3306)/npckefu_test' go test -race ./...   # 未设置则跳过需要数据库的测试

## 前端（内嵌，无需构建）
    SEED_DEMO=1 go run ./cmd/server     # 启动日志会打印演示租户的访客链接与坐席 token
- 访客：`/visitor.html?appkey=<appkey>`　坐席工作台：`/agent.html`（填 appkey/accid/token）

## REST
| 接口 | 认证 | 说明 |
|---|---|---|
| POST /v1/tenants | X-Admin-Token | 创建租户，返回 app_key/app_secret |
| POST /v1/accounts | X-App-Key/Secret | 注册/更新坐席或访客（返回 token）|
| POST /v1/visitors/register | body.app_key | 访客自助注册 |
| POST/GET/DELETE /v1/faqs | 租户 | 知识库 |
| GET /v1/sessions, /v1/sessions/{id}/messages?since= | 租户 | 会话/消息查询 |
| POST /v1/messages | 租户 | 服务端下发消息：`{session_id 或 to(访客accid), type, body, client_msg_id}`；同一 client_msg_id 幂等；发送者为 `business`(role=system)，客户端按通知样式展示；`to` 没有未关闭会话时自动新建 |
| GET /v1/accounts/{accid}/presence | 租户 | 在线状态：`{online, connections(同时在线端数), last_seen_at(离线时)}`，账号不存在返回 404 |
| POST /v1/presence | 租户 | 批量查询 `{accids:[…≤100]}`，按输入顺序返回，不存在的 `exists:false` |
| GET /v1/accounts/{accid}/recent?include_closed=1 | 租户 | 该账号的最近会话（最后消息预览、未读数），按最后活动时间倒序 |
| PUT/GET/DELETE /v1/webhook | 租户 | 配置消息回调地址；PUT `{url, rotate_secret?}` 返回签名密钥 `secret` |
| POST/GET /v1/sensitive-words, DELETE /v1/sensitive-words/{id} | 租户 | 敏感词库：POST `{words:[…], action:"mask"\|"block"}`（单词≥2字，每租户≤5000） |
| PUT /v1/accounts/{accid}/devices, DELETE …/devices/{token} | 租户 | 登记/删除推送设备 `{platform, token}`（也可由客户端经 WS `device` 自行登记） |

## WebSocket `/ws?appkey=&accid=&token=`
客户端 → 服务端 `op`: `start`(访客开始咨询) · `send`{session_id,client_msg_id,type,body} · `sync`{session_id,since_seq} · `close` · `typing` · `ping`
客户端另有：`recent`{include_closed}（最近会话+未读）· `read`{session_id,seq}（推进已读游标，不会超过最新 seq，并同步给同账号其他端）
服务端 → 客户端 `op`: `session` · `msg` · `ack` · `queue`{position} · `synced` · `recent` · `read` · `typing` · `error`

## 在线状态
- 在线 = 账号至少有一个 WebSocket 连接（多端同时在线时 `connections` > 1）；`last_seen_at` 是最后一次**断开**的时间，在线时不返回。
- `GET /v1/accounts?role=agent` 的每项也带 `online`，可直接用于坐席管理页。
- `presence.changed` 回调：`{accid, role, online, at(ms)}`。上线立即发；**下线要等 10 秒宽限期**，期间重连则不发（页面刷新、网络抖动不会刷屏）。
- ⚠️ 在线状态来自进程内连接表，**只有单实例部署时才准确**；多实例前需把连接表/心跳放到共享存储（Redis 等）。

## 敏感词过滤
访客/坐席发送的**文本**消息在落库前过滤（机器人回复与服务端下发的消息不过滤）：
- `mask`：命中部分替换为 `*`，**库里只存屏蔽后的文本**；`block`：整条拒绝，不落库，客户端收到 `error`。
- 匹配前归一化：忽略大小写、全角半角，以及词中夹杂的空格/标点/零宽字符（`傻 逼`、`傻.逼` 也会命中，屏蔽时整段替换）。
- 每次命中产生 `message.moderated` 回调事件（`action: masked|blocked`、命中词），可用于审计。
- **不覆盖**谐音、拼音、形近字、拆字等变体，也不处理图片；需要时请接入专业内容安全服务。
- 词库按租户缓存：本实例的增删立即生效，其他实例最长 30 秒后生效。词库读取失败时沿用旧缓存（没有则放行），优先保证聊天可用。

## 离线推送
收件人**不在线**（无任何 WebSocket 连接）且登记了设备时，产生 `push.offline` 回调事件：
    data: {"accid","session_id","seq","title","body"(前50字/[图片]),"devices":[{"platform","token"}]}
由**租户服务器**收到后转发到 APNs / 华为 / 小米 / OPPO / vivo / 荣耀 / FCM 等通道（本系统不内置厂商通道，也不持有任何推送证书）。
- 前提：租户已配置 webhook；未配置则推送事件不会产生。
- 同一会话对同一账号 30 秒内只推一次（合并），内部系统消息（如“会话已结束”）不推。
- 一个 token 同时只属于一个账号（同机换号登录时自动转移）；每账号最多保留最近 10 台设备。

## 消息回调（抄送）
配置 `PUT /v1/webhook` 后，以下事件会 POST 到你的地址：`message.created`（所有消息，含机器人/系统/服务端下发）、`message.moderated`、`push.offline`、`presence.changed`、`session.created` / `session.queued` / `session.assigned` / `session.closed`。
    POST <url>   {"event_id":123,"type":"message.created","created_at":<ms>,"data":{"session":{...},"message":{...}}}
    X-Kefu-Event-Id / X-Kefu-Event-Type / X-Kefu-Timestamp(秒) / X-Kefu-Signature: sha256=hex(HMAC_SHA256(secret, timestamp + "." + body))
- 返回 2xx 即成功；否则按 10s、30s、2m、10m、30m、1h、3h 退避重试，共 8 次后标记 dead。
- **至少一次**投递：请用 `event_id` 去重；重试可能乱序，需要顺序时按 `event_id` 排序。校验签名时请同时检查时间戳（防重放）。
- 事件先写入 MySQL 发件箱（`webhook_events`）再异步投递，进程崩溃不丢；多实例用 `SKIP LOCKED` 领取，互不重复。
- 安全：只允许 http(s)，**拨号时**拒绝回环/内网/链路本地/CGNAT 地址（防 SSRF 与 DNS rebinding），不跟随重定向，8 秒超时。本地调试可设 `WEBHOOK_ALLOW_PRIVATE=1`。

## 结构
cmd/server 入口 · internal/store 持久化 · internal/bot FAQ+LLM 应答 · internal/im 会话/路由/WS 网关 · internal/api REST

## 已知限制 / 下一步
- 单机内存 Hub；多实例需 Redis/NATS 做跨节点推送与在线状态。
- 存储为 MySQL 8（InnoDB/utf8mb4）；表结构在 store.schema 中自动创建，后续演进建议改用 golang-migrate 等迁移工具。
- 分配坐席的互斥锁是进程内的；多实例部署前需改为分布式锁（如 MySQL GET_LOCK 或 Redis）。
- 坐席掉线时其进行中会话不自动回收；缺少内置厂商推送通道、图片/语义级内容安全、会话评价、转接给指定坐席、富媒体上传、管理后台前端、限流。
- CheckOrigin 目前放开，需按租户域名白名单。
