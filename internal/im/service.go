// Package im 实现会话、消息投递、人工分配（路由/排队）与机器人接待流程。
package im

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"npckefu/internal/bot"
	"npckefu/internal/cluster"
	"npckefu/internal/moderation"
	"npckefu/internal/store"
)

var (
	ErrForbidden = errors.New("forbidden")
	ErrClosed    = errors.New("session closed")
	ErrBadInput  = errors.New("bad input")
	ErrSensitive = errors.New("消息包含违规内容，未发送")
)

const maxBotMisses = 2 // 连续未命中 N 次后自动转人工

type Service struct {
	St  *store.Store
	Hub *Hub
	Bot *bot.Bot
	Mod *moderation.Moderator
	Cl  cluster.Cluster // 多实例共享能力（在线状态/跨节点投递/分布式锁）；单实例用 cluster.Local
	// PushInterval 同一会话对同一离线账号的推送最小间隔（合并推送，避免连发消息轰炸），默认 30s。
	PushInterval time.Duration
	// PresenceGrace 账号最后一个连接断开后，等待多久仍未重连才对外宣告离线（吸收网络抖动/页面刷新），默认 10s。
	PresenceGrace time.Duration
	// SweepInterval 排队分配的兜底扫描间隔（任一时刻只有一个节点执行），默认 5s。
	SweepInterval time.Duration
}

// NewService 创建服务；cl 为 nil 时使用单进程的 cluster.Local。
func NewService(st *store.Store, b *bot.Bot, cl cluster.Cluster) *Service {
	if cl == nil {
		cl = cluster.NewLocal()
	}
	return &Service{St: st, Hub: NewHub(cl), Bot: b, Mod: moderation.New(st), Cl: cl,
		PushInterval: 30 * time.Second, PresenceGrace: 10 * time.Second, SweepInterval: 5 * time.Second}
}

// Run 运行后台任务直到 ctx 结束：集群消息订阅与心跳、排队分配的兜底扫描。
func (s *Service) Run(ctx context.Context) {
	go s.Hub.Run(ctx)
	tick := time.NewTicker(s.SweepInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			// 多节点同时运行时，每个周期只有一个节点执行扫描。
			if !s.Cl.Once(ctx, "dispatch-sweep", s.SweepInterval*8/10) {
				continue
			}
			ids, err := s.St.TenantsWithQueued(ctx)
			if err != nil {
				log.Printf("sweep: %v", err)
				continue
			}
			for _, id := range ids {
				s.Dispatch(ctx, id)
			}
		}
	}
}

// Shutdown 优雅停机：断开本进程所有连接，客户端会重连到其他节点。
func (s *Service) Shutdown() { s.Hub.CloseAll() }

type SendReq struct {
	SessionID   int64  `json:"session_id"`
	ClientMsgID string `json:"client_msg_id"`
	Type        string `json:"type"`
	Body        string `json:"body"`
}

func (in *SendReq) normalize() error {
	if in.Body == "" || len(in.Body) > 8192 || len(in.ClientMsgID) > 64 {
		return ErrBadInput
	}
	switch in.Type {
	case "":
		in.Type = "text"
	case "text", "image", "custom":
	default:
		return ErrBadInput
	}
	return nil
}

// emit 把事件写入回调发件箱（租户未配置回调时不落库）。
func (s *Service) emit(tenantID int64, typ string, data any) {
	b, err := json.Marshal(data)
	if err == nil {
		err = s.St.EnqueueEvent(context.Background(), tenantID, typ, b)
	}
	if err != nil {
		log.Printf("emit %s: %v", typ, err)
	}
}

// StartSession 访客进入咨询：复用未关闭会话，否则新建并由机器人问候。
func (s *Service) StartSession(ctx context.Context, acc *store.Account, skillGroup string) (*store.Session, error) {
	if acc.Role != store.RoleVisitor {
		return nil, ErrForbidden
	}
	sess, err := s.St.OpenSessionOfVisitor(ctx, acc.TenantID, acc.Accid)
	if err == nil {
		return sess, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	sess, err = s.St.CreateSession(ctx, acc.TenantID, acc.Accid, skillGroup)
	if err != nil {
		return nil, err
	}
	s.emit(sess.TenantID, "session.created", map[string]any{"session": sess})
	s.systemMsg(ctx, sess, store.RoleBot, "您好，我是智能客服小助手，请问有什么可以帮您？输入“转人工”可联系人工客服。")
	return sess, nil
}

// Send 处理一条来自访客/坐席的消息。
func (s *Service) Send(ctx context.Context, acc *store.Account, in SendReq) (*store.Message, error) {
	if err := in.normalize(); err != nil {
		return nil, err
	}
	sess, err := s.St.Session(ctx, acc.TenantID, in.SessionID)
	if err != nil {
		return nil, err
	}
	if sess.Status == store.StatusClosed {
		return nil, ErrClosed
	}
	switch acc.Role {
	case store.RoleVisitor:
		if sess.VisitorAccid != acc.Accid {
			return nil, ErrForbidden
		}
	case store.RoleAgent:
		if sess.AgentAccid != acc.Accid || sess.Status != store.StatusActive {
			return nil, ErrForbidden
		}
	default:
		return nil, ErrForbidden
	}
	if in.Type == "text" { // 敏感词：屏蔽则改写正文，拒绝则不落库
		if r := s.Mod.Check(ctx, acc.TenantID, in.Body); r.Hit {
			action := "masked"
			if r.Block {
				action = "blocked"
			}
			s.emit(acc.TenantID, "message.moderated", map[string]any{
				"session_id": sess.ID, "from": acc.Accid, "action": action, "words": r.Words,
			})
			if r.Block {
				return nil, ErrSensitive
			}
			in.Body = r.Text
		}
	}
	m, dup, err := s.St.AppendMessage(ctx, &store.Message{
		TenantID: acc.TenantID, SessionID: sess.ID, ClientMsgID: in.ClientMsgID,
		From: acc.Accid, FromRole: acc.Role, Type: in.Type, Body: in.Body,
	})
	if err != nil {
		return nil, err
	}
	if dup {
		return m, nil
	}
	s.deliver(sess, m)

	if acc.Role == store.RoleVisitor && in.Type == "text" {
		go s.afterVisitorMessage(sess, in.Body)
	}
	return m, nil
}

func (s *Service) afterVisitorMessage(sess *store.Session, text string) {
	ctx := context.Background()
	cur, err := s.St.Session(ctx, sess.TenantID, sess.ID)
	if err != nil || cur.Status != store.StatusBot {
		return // 已转人工/排队中：不再由机器人应答
	}
	if bot.WantsHuman(text) {
		s.Transfer(ctx, cur, "")
		return
	}
	r, err := s.Bot.Answer(ctx, cur.TenantID, text)
	if err != nil {
		log.Printf("bot answer: %v", err)
		return
	}
	if r.Source == "fallback" {
		n, _ := s.St.IncBotMisses(ctx, cur.ID, false)
		s.systemMsg(ctx, cur, store.RoleBot, r.Text)
		if n >= maxBotMisses {
			s.Transfer(ctx, cur, "")
		}
		return
	}
	_, _ = s.St.IncBotMisses(ctx, cur.ID, true)
	s.systemMsg(ctx, cur, store.RoleBot, r.Text)
}

// Transfer 转人工：进入排队并尝试立即分配。
func (s *Service) Transfer(ctx context.Context, sess *store.Session, skillGroup string) {
	if err := s.St.SetSessionStatus(ctx, sess.TenantID, sess.ID, store.StatusQueued, ""); err != nil {
		log.Printf("transfer: %v", err)
		return
	}
	sess.Status = store.StatusQueued
	s.emit(sess.TenantID, "session.queued", map[string]any{"session": sess})
	s.systemMsg(ctx, sess, store.RoleSystem, "正在为您转接人工客服，请稍候…")
	s.Dispatch(ctx, sess.TenantID)
}

// Dispatch 把排队会话分配给在线且未满载的坐席（技能组匹配，负载最小优先）。
func (s *Service) Dispatch(ctx context.Context, tenantID int64) {
	// 跨节点互斥：避免两个节点同时分配导致坐席超配。拿不到锁说明别的节点正在分配，
	// 若恰好漏掉了刚入队的会话，由 Run 里的兜底扫描在一个周期内补上。
	var unlock func()
	for i := 0; ; i++ {
		var ok bool
		if unlock, ok = s.Cl.Lock(ctx, fmt.Sprintf("dispatch:%d", tenantID), 15*time.Second); ok {
			break
		}
		if i >= 10 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	defer unlock()
	queued, err := s.St.QueuedSessions(ctx, tenantID)
	if err != nil || len(queued) == 0 {
		return
	}
	agents, err := s.St.ListAccounts(ctx, tenantID, store.RoleAgent)
	if err != nil {
		return
	}
	load, _ := s.St.ActiveLoad(ctx, tenantID)
	online := map[string]bool{} // 在线状态只查一次
	for _, a := range agents {
		online[a.Accid] = s.Hub.Online(tenantID, a.Accid)
	}
	for i := range queued {
		q := queued[i]
		var cands []store.Account
		for _, a := range agents {
			if !online[a.Accid] || load[a.Accid] >= a.MaxLoad {
				continue
			}
			if q.SkillGroup != "" && a.SkillGroup != q.SkillGroup {
				continue
			}
			cands = append(cands, a)
		}
		if len(cands) == 0 {
			s.notifyQueue(&q, i+1)
			continue
		}
		sort.Slice(cands, func(x, y int) bool { return load[cands[x].Accid] < load[cands[y].Accid] })
		a := cands[0]
		if err := s.St.SetSessionStatus(ctx, tenantID, q.ID, store.StatusActive, a.Accid); err != nil {
			continue
		}
		load[a.Accid]++
		q.Status, q.AgentAccid = store.StatusActive, a.Accid
		s.emit(tenantID, "session.assigned", map[string]any{"session": q})
		s.systemMsg(ctx, &q, store.RoleSystem, fmt.Sprintf("客服 %s 为您服务", a.Name))
		s.Hub.Push(tenantID, a.Accid, Event{Op: "session", Session: &q})
		s.Hub.Push(tenantID, q.VisitorAccid, Event{Op: "session", Session: &q})
	}
}

func (s *Service) notifyQueue(q *store.Session, pos int) {
	s.Hub.Push(q.TenantID, q.VisitorAccid, Event{Op: "queue", SessionID: q.ID, Position: pos})
}

// Close 关闭会话（访客或当前坐席），并释放名额触发排队分配。
func (s *Service) Close(ctx context.Context, acc *store.Account, sessionID int64) error {
	sess, err := s.St.Session(ctx, acc.TenantID, sessionID)
	if err != nil {
		return err
	}
	if sess.VisitorAccid != acc.Accid && sess.AgentAccid != acc.Accid {
		return ErrForbidden
	}
	if sess.Status == store.StatusClosed {
		return nil
	}
	if err := s.St.SetSessionStatus(ctx, sess.TenantID, sess.ID, store.StatusClosed, sess.AgentAccid); err != nil {
		return err
	}
	sess.Status = store.StatusClosed
	s.emit(sess.TenantID, "session.closed", map[string]any{"session": sess})
	s.systemMsg(ctx, sess, store.RoleSystem, "会话已结束")
	s.Hub.Push(sess.TenantID, sess.VisitorAccid, Event{Op: "session", Session: sess})
	if sess.AgentAccid != "" {
		s.Hub.Push(sess.TenantID, sess.AgentAccid, Event{Op: "session", Session: sess})
	}
	s.Dispatch(ctx, sess.TenantID)
	return nil
}

// Sync 拉取 seq 之后的消息，校验参与者。
func (s *Service) Sync(ctx context.Context, acc *store.Account, sessionID, since int64) ([]store.Message, error) {
	sess, err := s.St.Session(ctx, acc.TenantID, sessionID)
	if err != nil {
		return nil, err
	}
	if sess.VisitorAccid != acc.Accid && sess.AgentAccid != acc.Accid {
		return nil, ErrForbidden
	}
	return s.St.MessagesSince(ctx, acc.TenantID, sessionID, since, 500)
}

func (s *Service) systemMsg(ctx context.Context, sess *store.Session, role, text string) {
	from := "system"
	if role == store.RoleBot {
		from = "bot"
	}
	m, _, err := s.St.AppendMessage(ctx, &store.Message{TenantID: sess.TenantID, SessionID: sess.ID, From: from, FromRole: role, Type: "text", Body: text})
	if err != nil {
		log.Printf("system msg: %v", err)
		return
	}
	s.deliver(sess, m)
}

func (s *Service) deliver(sess *store.Session, m *store.Message) {
	s.emit(sess.TenantID, "message.created", map[string]any{"session": sess, "message": m})
	s.pushOffline(sess, m)
	ev := Event{Op: "msg", Message: m}
	s.Hub.Push(sess.TenantID, sess.VisitorAccid, ev)
	if sess.AgentAccid != "" {
		s.Hub.Push(sess.TenantID, sess.AgentAccid, ev)
	}
}

// MarkRead 推进已读游标（不超过会话最新 seq），并通知该账号的其他在线端。
func (s *Service) MarkRead(ctx context.Context, acc *store.Account, sessionID, seq int64) error {
	sess, err := s.St.Session(ctx, acc.TenantID, sessionID)
	if err != nil {
		return err
	}
	if sess.VisitorAccid != acc.Accid && sess.AgentAccid != acc.Accid {
		return ErrForbidden
	}
	if seq > sess.LastSeq {
		seq = sess.LastSeq
	}
	if seq <= 0 {
		return ErrBadInput
	}
	if err := s.St.MarkRead(ctx, sessionID, acc.Accid, seq); err != nil {
		return err
	}
	s.Hub.Push(acc.TenantID, acc.Accid, Event{Op: "read", SessionID: sessionID, Seq: seq})
	return nil
}

// Recent 最近会话列表（带最后一条消息预览与未读数）。
func (s *Service) Recent(ctx context.Context, acc *store.Account, includeClosed bool, limit int) ([]store.RecentItem, error) {
	return s.St.RecentSessions(ctx, acc.TenantID, acc.Accid, includeClosed, limit)
}

// BusinessSender 是租户服务端经 REST 下发的消息的发送者标识（role 为 system，客户端按通知样式展示）。
const BusinessSender = "business"

// SendNotice 租户服务端向会话/访客下发消息（订单通知、人工补充说明等）。
// 指定 sessionID，或指定访客 accid（复用其未关闭会话，没有则新建）。client_msg_id 可用于幂等。
func (s *Service) SendNotice(ctx context.Context, tenantID, sessionID int64, to string, in SendReq) (*store.Message, error) {
	if err := in.normalize(); err != nil {
		return nil, err
	}
	var sess *store.Session
	var err error
	switch {
	case sessionID != 0:
		sess, err = s.St.Session(ctx, tenantID, sessionID)
	case to != "":
		var acc *store.Account
		if acc, err = s.St.Account(ctx, tenantID, to); err != nil {
			return nil, err
		}
		if acc.Role != store.RoleVisitor {
			return nil, ErrBadInput
		}
		if sess, err = s.St.OpenSessionOfVisitor(ctx, tenantID, to); errors.Is(err, store.ErrNotFound) {
			if sess, err = s.St.CreateSession(ctx, tenantID, to, ""); err == nil {
				s.emit(tenantID, "session.created", map[string]any{"session": sess})
			}
		}
	default:
		return nil, ErrBadInput
	}
	if err != nil {
		return nil, err
	}
	if sess.Status == store.StatusClosed {
		return nil, ErrClosed
	}
	m, dup, err := s.St.AppendMessage(ctx, &store.Message{
		TenantID: tenantID, SessionID: sess.ID, ClientMsgID: in.ClientMsgID,
		From: BusinessSender, FromRole: store.RoleSystem, Type: in.Type, Body: in.Body,
	})
	if err != nil {
		return nil, err
	}
	if !dup {
		s.deliver(sess, m)
	}
	return m, nil
}

// pushOffline 收件人不在线且登记了设备时，产出 push.offline 事件（经回调发件箱交给租户服务器转发到
// APNs/厂商通道/FCM）。同一会话对同一账号在 PushInterval 内只推一次。内部系统消息不推送。
func (s *Service) pushOffline(sess *store.Session, m *store.Message) {
	if m.From == "system" {
		return
	}
	for _, to := range []string{sess.VisitorAccid, sess.AgentAccid} {
		if to == "" || to == m.From || s.Hub.Online(sess.TenantID, to) {
			continue
		}
		devs, err := s.St.DevicesOf(context.Background(), sess.TenantID, to)
		if err != nil {
			log.Printf("push devices: %v", err)
			continue
		}
		if len(devs) == 0 {
			continue
		}
		// 跨节点合并推送：同一会话对同一账号在 PushInterval 内只推一次。
		if !s.Cl.Once(context.Background(), fmt.Sprintf("push:%d:%d:%s", sess.TenantID, sess.ID, to), s.PushInterval) {
			continue
		}
		title := "您有新的客服消息"
		if to == sess.AgentAccid {
			title = "访客新消息"
		}
		s.emit(sess.TenantID, "push.offline", map[string]any{
			"accid": to, "session_id": sess.ID, "seq": m.Seq, "title": title, "body": previewOf(m), "devices": devs,
		})
	}
}

func previewOf(m *store.Message) string {
	switch m.Type {
	case "text":
		r := []rune(m.Body)
		if len(r) > 50 {
			r = r[:50]
		}
		return string(r)
	case "image":
		return "[图片]"
	}
	return "[新消息]"
}

// RegisterDevice 登记推送设备（账号自己登记或租户服务端代登记）。
func (s *Service) RegisterDevice(ctx context.Context, acc *store.Account, platform, token string) error {
	if platform == "" || len(platform) > 16 || token == "" || len(token) > 512 {
		return ErrBadInput
	}
	return s.St.UpsertDevice(ctx, acc.TenantID, acc.Accid, platform, token)
}

// ---- 在线状态 ----

func presenceKey(a *store.Account) string { return fmt.Sprintf("%d/%s", a.TenantID, a.Accid) }

func (s *Service) emitPresence(a *store.Account, online bool) {
	s.emit(a.TenantID, "presence.changed", map[string]any{"accid": a.Accid, "role": a.Role, "online": online, "at": time.Now().UnixMilli()})
}

// presenceUp 账号在全局范围内第一次上线。"已宣告在线"标记存在共享存储里：若上次断线仍在宽限期内
// （标记还在），不重复宣告；跨节点重连同理。
func (s *Service) presenceUp(a *store.Account) {
	if s.Cl.Announce(context.Background(), presenceKey(a), true) {
		s.emitPresence(a, true)
	}
}

// presenceDown 账号在全局范围内最后一个连接断开：立即记录最后在线时间，宽限期后仍无连接才宣告离线。
// 只有成功清除"已宣告在线"标记的那个节点会发出离线事件，不会重复。
func (s *Service) presenceDown(a *store.Account) {
	if err := s.St.TouchLastSeen(context.Background(), a.TenantID, a.Accid); err != nil {
		log.Printf("last_seen: %v", err)
	}
	time.AfterFunc(s.PresenceGrace, func() {
		ctx := context.Background()
		if s.Hub.Online(a.TenantID, a.Accid) {
			return
		}
		if !s.Cl.Announce(ctx, presenceKey(a), false) {
			return
		}
		if s.Hub.Online(a.TenantID, a.Accid) { // 清标记的瞬间又重连了：恢复标记，不报离线
			s.Cl.Announce(ctx, presenceKey(a), true)
			return
		}
		s.emitPresence(a, false)
	})
}

type Presence struct {
	Accid       string     `json:"accid"`
	Exists      bool       `json:"exists"`
	Role        string     `json:"role,omitempty"`
	Online      bool       `json:"online"`
	Connections int        `json:"connections"` // 同时在线的端数
	LastSeenAt  *time.Time `json:"last_seen_at,omitempty"`
}

// Presences 批量查询，按输入顺序返回；不存在的账号 exists=false。
// 注意：在线状态来自本进程的连接表，多实例部署需改为共享存储（见 README）。
func (s *Service) Presences(ctx context.Context, tenantID int64, accids []string) ([]Presence, error) {
	rows, err := s.St.PresenceRows(ctx, tenantID, accids)
	if err != nil {
		return nil, err
	}
	out := make([]Presence, 0, len(accids))
	for _, id := range accids {
		p := Presence{Accid: id}
		if r, ok := rows[id]; ok {
			p.Exists, p.Role = true, r.Role
			p.Connections = s.Hub.Count(tenantID, id)
			p.Online = p.Connections > 0
			if !p.Online { // 在线时不返回 last_seen，避免与"现在"混淆
				p.LastSeenAt = r.LastSeenAt
			}
		}
		out = append(out, p)
	}
	return out, nil
}
