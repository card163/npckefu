package im

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"npckefu/internal/store"
)

var (
	ErrBadTarget         = errors.New("目标坐席或技能组不存在")
	ErrTargetUnavailable = errors.New("目标坐席不在线或已满载")
	ErrConflict          = errors.New("会话状态已变化，请刷新后重试")
	ErrTooManyTransfers  = errors.New("该会话转接次数过多")
)

// isUserError 判断错误是否可以原样返回给客户端（其余一律显示为 internal error，细节只进日志）。
func isUserError(err error) bool {
	for _, e := range []error{ErrForbidden, ErrBadInput, ErrClosed, ErrSensitive, ErrBadFile, ErrNotClosed, ErrRateExpired,
		ErrAlreadyRated, ErrBadTarget, ErrTargetUnavailable, ErrConflict, ErrTooManyTransfers, store.ErrNotFound} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

const (
	maxTransfersPerSession = 10
	maxNoteRunes           = 500
)

// TransferReq：ToAgent 指定坐席；SkillGroup 转到技能组（重新排队）；都为空则退回队列交给其他坐席。两者不能同时给。
type TransferReq struct {
	ToAgent    string
	SkillGroup string
	Note       string // 仅接手坐席可见，不会出现在访客的消息里
}

type RosterItem struct {
	Accid      string `json:"accid"`
	Name       string `json:"name"`
	SkillGroup string `json:"skill_group"`
	Online     bool   `json:"online"`
	Load       int    `json:"load"`
	MaxLoad    int    `json:"max_load"`
}

// Roster 坐席名单（在线状态与当前接待数），在线的排前面。
func (s *Service) Roster(ctx context.Context, tenantID int64) ([]RosterItem, error) {
	agents, err := s.St.ListAccounts(ctx, tenantID, store.RoleAgent)
	if err != nil {
		return nil, err
	}
	load, err := s.St.ActiveLoad(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	out := make([]RosterItem, 0, len(agents))
	for _, a := range agents {
		out = append(out, RosterItem{a.Accid, a.Name, a.SkillGroup, s.Hub.Online(tenantID, a.Accid), load[a.Accid], a.MaxLoad})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Online && !out[j].Online })
	return out, nil
}

// TransferAgent 转接会话。actor 为发起的坐席 accid（必须是会话当前的接待坐席）；租户服务端代为操作时传空串。
//
// 并发安全：改派是带条件的 UPDATE（仍为 active 且仍属于原坐席），与"结束会话""另一次转接"并发时只有一方成功；
// 直接转给指定坐席时，容量检查与改派在同一把分配锁内完成，不会把目标坐席转超。
func (s *Service) TransferAgent(ctx context.Context, tenantID int64, actor string, sessionID int64, req TransferReq) (*store.Session, error) {
	req.Note = strings.TrimSpace(req.Note)
	if utf8.RuneCountInString(req.Note) > maxNoteRunes || (req.ToAgent != "" && req.SkillGroup != "") {
		return nil, ErrBadInput
	}
	sess, err := s.St.Session(ctx, tenantID, sessionID)
	if err != nil {
		return nil, err
	}
	switch {
	case sess.Status == store.StatusClosed:
		return nil, ErrClosed
	case sess.Status != store.StatusActive || sess.AgentAccid == "":
		return nil, ErrConflict
	case actor != "" && sess.AgentAccid != actor:
		return nil, ErrForbidden
	}
	if n, err := s.St.CountTransfers(ctx, tenantID, sessionID); err != nil {
		return nil, err
	} else if n >= maxTransfersPerSession {
		return nil, ErrTooManyTransfers
	}
	from := sess.AgentAccid
	tr := &store.Transfer{SessionID: sessionID, FromAgent: from, Note: req.Note}

	var target *store.Account
	switch {
	case req.ToAgent != "":
		if req.ToAgent == from {
			return nil, ErrBadInput
		}
		if target, err = s.St.Account(ctx, tenantID, req.ToAgent); errors.Is(err, store.ErrNotFound) || (err == nil && target.Role != store.RoleAgent) {
			return nil, ErrBadTarget
		} else if err != nil {
			return nil, err
		}
		unlock, ok := s.lockDispatch(ctx, tenantID)
		if !ok {
			return nil, ErrConflict
		}
		defer unlock()
		load, err := s.St.ActiveLoad(ctx, tenantID)
		if err != nil {
			return nil, err
		}
		if !s.Hub.Online(tenantID, target.Accid) || load[target.Accid] >= target.MaxLoad {
			return nil, ErrTargetUnavailable
		}
		tr.ToAgent = target.Accid
	case req.SkillGroup != "":
		if ok, err := s.St.AgentGroupExists(ctx, tenantID, req.SkillGroup); err != nil {
			return nil, err
		} else if !ok {
			return nil, ErrBadTarget
		}
		tr.SkillGroup = req.SkillGroup
	}

	ok, err := s.St.ApplyTransfer(ctx, tenantID, tr, tr.ToAgent, tr.SkillGroup)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrConflict
	}
	cur, err := s.St.Session(ctx, tenantID, sessionID)
	if err != nil {
		return nil, err
	}
	s.emit(tenantID, "session.transferred", map[string]any{
		"session": cur, "from_agent": from, "to_agent": tr.ToAgent, "skill_group": tr.SkillGroup, "note": req.Note,
	})
	// 原坐席：会话已不再属于他（客户端据此移除）
	s.Hub.Push(tenantID, from, Event{Op: "session", Session: cur})

	if target != nil {
		fromName := from
		if a, err := s.St.Account(ctx, tenantID, from); err == nil && a.Name != "" {
			fromName = a.Name
		}
		s.systemMsg(ctx, cur, store.RoleSystem, fmt.Sprintf("客服 %s 已将您转接给客服 %s", fromName, target.Name))
		s.Hub.Push(tenantID, target.Accid, Event{Op: "session", Session: cur})
		s.Hub.Push(tenantID, target.Accid, Event{Op: "transferred", SessionID: sessionID, From: from, Note: req.Note})
		s.Hub.Push(tenantID, cur.VisitorAccid, Event{Op: "session", Session: cur})
		return cur, nil
	}
	// 退回队列/技能组：先告知访客，再尝试立即分配（排除原坐席）
	s.systemMsg(ctx, cur, store.RoleSystem, "正在为您转接其他客服，请稍候…")
	s.Hub.Push(tenantID, cur.VisitorAccid, Event{Op: "session", Session: cur})
	s.Dispatch(ctx, tenantID)
	return cur, nil
}
