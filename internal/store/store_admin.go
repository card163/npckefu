package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// ---- 会话检索（管理后台）----

type SessionFilter struct {
	Status   string
	Agent    string
	Visitor  string
	BeforeID int64 // 翻页：只取 id < BeforeID
	Limit    int
}

func (s *Store) FindSessions(ctx context.Context, tenantID int64, f SessionFilter) ([]Session, error) {
	if f.Limit <= 0 || f.Limit > 200 {
		f.Limit = 50
	}
	if f.BeforeID == 0 {
		f.BeforeID = 1<<62 - 1
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+sessCols+` FROM sessions WHERE tenant_id=? AND (?='' OR status=?) AND (?='' OR agent_accid=?) AND (?='' OR visitor_accid=?) AND id<? ORDER BY id DESC LIMIT ?`,
		tenantID, f.Status, f.Status, f.Agent, f.Agent, f.Visitor, f.Visitor, f.BeforeID, f.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Session
	for rows.Next() {
		x, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *x)
	}
	return out, rows.Err()
}

// ---- 概览统计 ----

// LiveCounts 当前各状态的会话数（bot/queued/active）。
func (s *Store) LiveCounts(ctx context.Context, tenantID int64) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT status,COUNT(*) FROM sessions WHERE tenant_id=? AND status IN ('bot','queued','active') GROUP BY status`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{"bot": 0, "queued": 0, "active": 0}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}

// CountSince 自 since 起新建的会话数、其中有人工接待的会话数、访客与坐席发送的消息数。
func (s *Store) CountSince(ctx context.Context, tenantID int64, since time.Time) (sessions, human, messages int, err error) {
	if err = s.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(agent_accid<>''),0) FROM sessions WHERE tenant_id=? AND created_at>=?`, tenantID, since.UTC()).Scan(&sessions, &human); err != nil {
		return
	}
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE tenant_id=? AND created_at>=? AND from_role IN ('visitor','agent')`, tenantID, since.UTC()).Scan(&messages)
	return
}

// ---- FAQ 更新 ----

func (s *Store) UpdateFAQ(ctx context.Context, f *FAQ) error {
	r, err := s.db.ExecContext(ctx, `UPDATE faqs SET question=?, answer=?, keywords=? WHERE tenant_id=? AND id=?`, f.Question, f.Answer, f.Keywords, f.TenantID, f.ID)
	if err != nil {
		return err
	}
	if n, _ := r.RowsAffected(); n == 0 {
		// 内容未变时 RowsAffected 也为 0，再确认是否存在
		var x int64
		if err := s.db.QueryRowContext(ctx, `SELECT id FROM faqs WHERE tenant_id=? AND id=?`, f.TenantID, f.ID).Scan(&x); errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
	}
	return nil
}

// ---- 令牌重置 ----

// ResetToken 为账号生成新的登录令牌。
func (s *Store) ResetToken(ctx context.Context, tenantID int64, accid string) (string, error) {
	tok := RandHex(16)
	r, err := s.db.ExecContext(ctx, `UPDATE accounts SET token=? WHERE tenant_id=? AND accid=?`, tok, tenantID, accid)
	if err != nil {
		return "", err
	}
	if n, _ := r.RowsAffected(); n == 0 {
		return "", ErrNotFound
	}
	return tok, nil
}

// ---- 回调投递记录 ----

type WebhookEventRow struct {
	ID        int64     `json:"id"`
	Type      string    `json:"type"`
	Status    string    `json:"status"` // pending | done | dead | dropped
	Attempts  int       `json:"attempts"`
	LastError string    `json:"last_error"`
	NextAt    time.Time `json:"next_at"`
	CreatedAt time.Time `json:"created_at"`
}

func (s *Store) ListWebhookEvents(ctx context.Context, tenantID int64, status string, beforeID int64, limit int) ([]WebhookEventRow, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if beforeID == 0 {
		beforeID = 1<<62 - 1
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,event_type,status,attempts,last_error,next_at,created_at FROM webhook_events WHERE tenant_id=? AND (?='' OR status=?) AND id<? ORDER BY id DESC LIMIT ?`,
		tenantID, status, status, beforeID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WebhookEventRow
	for rows.Next() {
		var e WebhookEventRow
		if err := rows.Scan(&e.ID, &e.Type, &e.Status, &e.Attempts, &e.LastError, &e.NextAt, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// RetryWebhookEvent 把投递失败（dead）的事件重新放回队列；其他状态返回 ErrNotFound。
func (s *Store) RetryWebhookEvent(ctx context.Context, tenantID, id int64) error {
	r, err := s.db.ExecContext(ctx, `UPDATE webhook_events SET status='pending', attempts=0, last_error='', next_at=NOW(3) WHERE tenant_id=? AND id=? AND status='dead'`, tenantID, id)
	if err != nil {
		return err
	}
	if n, _ := r.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}
