package im

import (
	"context"
	"log"
	"strconv"
	"strings"

	"npckefu/internal/store"
)

// queueWatch 检查某租户的排队会话：到点提醒访客、超时告警坐席与租户、必要时放弃排队。
// 由 Run 的兜底扫描驱动（多节点时每个周期只有一个节点执行；各动作还带"认领"条件，绝不会重复）。
func (s *Service) queueWatch(ctx context.Context, tenantID int64) {
	set, err := s.St.QueueSettings(ctx, tenantID)
	if err != nil {
		log.Printf("queue settings: %v", err)
		return
	}
	if set.RemindAfter == 0 && set.AlertAfter == 0 && set.GiveupAfter == 0 {
		return
	}
	list, err := s.St.QueuedForWatch(ctx, tenantID)
	if err != nil {
		log.Printf("queue watch: %v", err)
		return
	}
	for i := range list {
		q := &list[i]
		sess := q.Session
		position := i + 1 // 先来先服务：按排队起点排序后的位置
		switch {
		case set.GiveupAfter > 0 && q.WaitSecs >= set.GiveupAfter:
			s.queueGiveup(ctx, tenantID, &sess, q.WaitSecs, set)
			continue
		}
		if set.AlertAfter > 0 && !q.Alerted && q.WaitSecs >= set.AlertAfter {
			if ok, err := s.St.ClaimAlert(ctx, tenantID, sess.ID); err == nil && ok {
				s.queueAlert(ctx, tenantID, &sess, q.WaitSecs, position, len(list))
			}
		}
		if set.RemindAfter > 0 && q.Reminders < set.RemindMax && q.WaitSecs >= set.RemindAfter+q.Reminders*set.RemindEvery {
			if ok, err := s.St.ClaimReminder(ctx, tenantID, sess.ID, q.Reminders); err == nil && ok {
				text := strings.NewReplacer("{position}", strconv.Itoa(position), "{ahead}", strconv.Itoa(position-1)).Replace(set.RemindMessage)
				s.systemMsg(ctx, &sess, store.RoleSystem, text)
				s.Hub.Push(tenantID, sess.VisitorAccid, Event{Op: "queue", SessionID: sess.ID, Position: position})
			}
		}
	}
}

// queueAlert 排队超时：通知所有在线坐席（让有空的人接待），并向租户发 session.queue_timeout 回调。
func (s *Service) queueAlert(ctx context.Context, tenantID int64, sess *store.Session, wait, position, queueLen int) {
	s.emit(tenantID, "session.queue_timeout", map[string]any{"session": sess, "waited_seconds": wait, "position": position, "queue_length": queueLen})
	agents, err := s.St.ListAccounts(ctx, tenantID, store.RoleAgent)
	if err != nil {
		return
	}
	for _, a := range agents {
		if sess.SkillGroup != "" && a.SkillGroup != sess.SkillGroup {
			continue
		}
		s.Hub.Push(tenantID, a.Accid, Event{Op: "queue_alert", SessionID: sess.ID, Wait: wait, Position: position})
	}
}

// queueGiveup 放弃排队：退回机器人，告知访客，并向租户发 session.queue_giveup 回调。
func (s *Service) queueGiveup(ctx context.Context, tenantID int64, sess *store.Session, wait int, set store.QueueSettings) {
	ok, err := s.St.ClaimGiveup(ctx, tenantID, sess.ID)
	if err != nil || !ok {
		return
	}
	cur, err := s.St.Session(ctx, tenantID, sess.ID)
	if err != nil {
		return
	}
	s.emit(tenantID, "session.queue_giveup", map[string]any{"session": cur, "waited_seconds": wait})
	s.systemMsg(ctx, cur, store.RoleSystem, set.GiveupMessage)
	s.Hub.Push(tenantID, cur.VisitorAccid, Event{Op: "session", Session: cur})
}
