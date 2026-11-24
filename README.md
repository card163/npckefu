# npckefu — 智能客服 SaaS（Go 后端）

多租户智能客服后端：机器人优先接待 → 转人工 → 技能组/负载路由 → 排队。IM 模型参考云信 IM 2.0：
`appkey/appsecret` 租户、服务端注册 `accid`+`token`、客户端 WebSocket 登录、会话内递增 `seq`、
`client_msg_id` 幂等去重、`ack`、断线按 `since_seq` 同步。

## 运行
    docker compose up -d                          # 本地 MySQL 8（库 npckefu）
    DB_DSN='root:@tcp(127.0.0.1:3306)/npckefu' ADMIN_TOKEN=xxx go run ./cmd/server   # :8080，启动时自动建表
    # 可选 LLM（OpenAI 兼容）：LLM_API_KEY / LLM_BASE_URL / LLM_MODEL
    TEST_MYSQL_DSN='root:@tcp(127.0.0.1:3306)/npckefu_test' go test -race ./...   # 未设置则跳过需要数据库的测试

## 多实例部署
设置 `REDIS_ADDR`（可选 `REDIS_PASSWORD` / `REDIS_DB` / `NODE_ID`）即启用集群模式；不设置则为单实例模式，行为与之前一致。所有实例共用同一个 MySQL 和 Redis，前面放负载均衡（WebSocket 无需会话保持）。

    docker compose up -d       # mysql + redis
    REDIS_ADDR=127.0.0.1:6379 NODE_ID=n1 ADDR=:8081 go run ./cmd/server
    REDIS_ADDR=127.0.0.1:6379 NODE_ID=n2 ADDR=:8082 go run ./cmd/server

| 能力 | 单实例问题 | 多实例做法 |
|---|---|---|
| 在线状态 | 只存在进程内 | Redis ZSET，每连接一项（`节点\|连接ID`），分数为过期时间（取 Redis 服务器时间，无时钟偏差）；节点每 10s 心跳续期，崩溃后最多 30s 失效 |
| 跨节点投递 | 只能推给本进程的连接 | 发送方查账号所在节点，经 Redis pub/sub（每节点一个频道）转发；断线客户端仍靠 `seq` 补拉，不依赖投递必达 |
| 坐席分配 | 进程内互斥锁 | Redis 分布式锁（带 TTL）；另有兜底扫描（每 5s，全局只有一个节点执行），拿不到锁时漏掉的排队会话会被补上 |
| 在线/离线事件 | 进程内去重 | Redis 里的“已宣告”标记，跨节点只发一次（含 10s 宽限期） |
| 推送合并 | 进程内计时 | Redis `SET NX PX` |
| 回调投递 | — | 已是 MySQL `SKIP LOCKED`，天然多实例 |

- `GET /readyz`：检查 MySQL（及 Redis），供负载均衡/K8s 就绪探针；`/healthz` 仅表示进程存活。
- 优雅停机（SIGTERM）：先停止接收新连接，再以 close code 1001 主动断开所有 WebSocket，客户端自动重连到其他节点；在线状态立即正确。
- 每个进程的 `NODE_ID` 必须唯一（默认 主机名+随机串）。Redis 故障时（按代码行为说明，**未做故障注入测试**）：同一节点上的访客与坐席仍能互发消息，在线状态回退为只看本节点，跨节点投递中断；**坐席分配会暂停**（分布式锁拿不到即放弃，宁可不分配也不超配），Redis 恢复后心跳会重新登记连接、分配自动继续。

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
| POST /v1/upload | 账号 `X-App-Key`+`X-Accid`+`X-Token`，或租户 `X-App-Key`+`X-App-Secret` | 上传图片（multipart，字段 `file`），返回 `{file_id,url,width,height,size,mime}` |
| GET /files/{id} | 无（持有链接） | 读取图片 |
| PUT/GET/DELETE /v1/webhook | 租户 | 配置消息回调地址；PUT `{url, rotate_secret?}` 返回签名密钥 `secret` |
| POST/GET /v1/sensitive-words, DELETE /v1/sensitive-words/{id} | 租户 | 敏感词库：POST `{words:[…], action:"mask"\|"block"}`（单词≥2字，每租户≤5000） |
| PUT /v1/accounts/{accid}/devices, DELETE …/devices/{token} | 租户 | 登记/删除推送设备 `{platform, token}`（也可由客户端经 WS `device` 自行登记） |

## WebSocket `/ws?appkey=&accid=&token=`
客户端 → 服务端 `op`: `start`(访客开始咨询) · `send`{session_id,client_msg_id,type,body} · `sync`{session_id,since_seq} · `close` · `typing` · `ping`
客户端另有：`recent`{include_closed}（最近会话+未读）· `read`{session_id,seq}（推进已读游标，不会超过最新 seq，并同步给同账号其他端）
服务端 → 客户端 `op`: `session` · `msg` · `ack` · `queue`{position} · `synced` · `recent` · `read` · `typing` · `error`

## 图片
1. `POST /v1/upload` 上传，拿到 `file_id`；2. 发送 `{"op":"send","session_id":…,"type":"image","body":"{\"file_id\":\"…\"}"}`（租户服务端用 `POST /v1/messages`，同样 `type:"image"`）；3. 收到的消息 `type:"image"`，`body` 是服务端生成的 JSON：`{file_id,url:"/files/<id>",width,height,size,mime}`，客户端用 `url` 渲染（内置两个页面已支持：📎 选图、粘贴图片、点击看原图）。
- **校验**：按文件**内容**判断类型，只允许 JPEG/PNG/GIF/WebP（拒绝 SVG/HTML 等可含脚本的格式、伪造扩展名的文件）；≤5MB，单边≤10000px、总像素≤5000 万（防“解压炸弹”，只读图片头，不解码整图）；每个账号每分钟最多上传 30 次（超出 429）。
- **只能引用自己的上传**：图片消息的 `file_id` 必须是**本人**上传、同租户、且未被绑定到别的会话；正文由服务端重写，客户端无法塞入外链或他人文件。
- **读取**：文件 ID 是 128 位随机数，充当持有链接（`<img>` 带不了认证头）；响应带 `nosniff`、`CSP: sandbox`，内容不可变可长期缓存。**知道链接的人都能看**，这与多数 IM 的做法一致；需要更严格的访问控制时，应改为带过期时间的签名链接。
- **清理**：上传后 24 小时内未被任何消息引用的文件会被定期删除（多实例下每小时只有一个节点执行）；**已发送的图片目前不会自动过期**。
- **存储** `STORAGE=db`（默认，存 MySQL，多实例开箱即用，适合中小规模；MySQL 的 `max_allowed_packet` 需≥8MB，8.0 默认 64MB）｜`fs`（`UPLOAD_DIR`，多实例必须是共享卷）｜`off`（关闭图片功能，接口返回 404）。对象存储（S3/OSS/COS）未内置：实现 `files.Storage` 接口即可。
- 图片消息不做敏感词过滤，也不做图片内容审核；不会剥离 JPEG 的 EXIF（可能含拍摄位置），也不生成缩略图（前端按宽度缩小显示）。

## 在线状态
- 在线 = 账号至少有一个 WebSocket 连接（多端同时在线时 `connections` > 1）；`last_seen_at` 是最后一次**断开**的时间，在线时不返回。
- `GET /v1/accounts?role=agent` 的每项也带 `online`，可直接用于坐席管理页。
- `presence.changed` 回调：`{accid, role, online, at(ms)}`。上线立即发；**下线要等 10 秒宽限期**，期间重连则不发（页面刷新、网络抖动不会刷屏）。
- 多实例下在线状态是**全局**的（见“多实例部署”）。

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
- 存储为 MySQL 8（InnoDB/utf8mb4）；表结构在 store.schema 中自动创建，后续演进建议改用 golang-migrate 等迁移工具。
- 多实例下，节点**崩溃**（非优雅停机）时，其上的连接在 30 秒 TTL 后自动失效，在线状态查询随之正确，但**不会产生** `presence.changed` 离线事件、也不更新 `last_seen_at`（没有节点观察到“断开”）。
- 坐席掉线时其进行中会话不自动回收；缺少内置厂商推送通道、图片内容审核、对象存储、会话评价、转接给指定坐席、语音/视频/文件消息、管理后台前端、全局接口限流（目前只有上传限流）。
- CheckOrigin 目前放开，需按租户域名白名单。
