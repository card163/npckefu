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
| GET /v1/accounts/{accid}/recent?include_closed=1 | 租户 | 该账号的最近会话（最后消息预览、未读数），按最后活动时间倒序 |
| PUT/GET/DELETE /v1/webhook | 租户 | 配置消息回调地址；PUT `{url, rotate_secret?}` 返回签名密钥 `secret` |

## WebSocket `/ws?appkey=&accid=&token=`
客户端 → 服务端 `op`: `start`(访客开始咨询) · `send`{session_id,client_msg_id,type,body} · `sync`{session_id,since_seq} · `close` · `typing` · `ping`
客户端另有：`recent`{include_closed}（最近会话+未读）· `read`{session_id,seq}（推进已读游标，不会超过最新 seq，并同步给同账号其他端）
服务端 → 客户端 `op`: `session` · `msg` · `ack` · `queue`{position} · `synced` · `recent` · `read` · `typing` · `error`

## 消息回调（抄送）
配置 `PUT /v1/webhook` 后，以下事件会 POST 到你的地址：`message.created`（所有消息，含机器人/系统/服务端下发）、`session.created` / `session.queued` / `session.assigned` / `session.closed`。
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
- 坐席掉线时其进行中会话不自动回收；缺少离线推送、内容安全（敏感词）、用户在线状态查询接口、会话评价、转接给指定坐席、富媒体上传、管理后台前端、限流。
- CheckOrigin 目前放开，需按租户域名白名单。
