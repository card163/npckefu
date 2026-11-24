// Package im 实现会话、消息投递、人工分配（路由/排队）与机器人接待流程。
package im

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"

	"npckefu/internal/bot"
	"npckefu/internal/store"
)

var (
	ErrForbidden = errors.New("forbidden")
	ErrClosed    = errors.New("session closed")
	ErrBadInput  = errors.New("bad input")
)

const maxBotMisses = 2 // 连续未命中 N 次后自动转人工

type Service struct {
	St  *store.Store
	Hub *Hub
	Bot *bot.Bot
	mu  sync.Mutex // 串行化分配，避免超配坐席
}

func NewService(st *store.Store, b *bot.Bot) *Service {
	return &Service{St: st, Hub: NewHub(), Bot: b}
}

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
	s.mu.Lock()
	defer s.mu.Unlock()
	queued, err := s.St.QueuedSessions(ctx, tenantID)
	if err != nil || len(queued) == 0 {
		return
	}
	agents, err := s.St.ListAccounts(ctx, tenantID, store.RoleAgent)
	if err != nil {
		return
	}
	load, _ := s.St.ActiveLoad(ctx, tenantID)
	for i := range queued {
		q := queued[i]
		var cands []store.Account
		for _, a := range agents {
			if !s.Hub.Online(tenantID, a.Accid) || load[a.Accid] >= a.MaxLoad {
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
