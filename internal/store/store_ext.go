package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// ---- 回调配置 ----

type Webhook struct {
	URL    string `json:"url"`
	Secret string `json:"secret,omitempty"`
}

func (s *Store) GetWebhook(ctx context.Context, tenantID int64) (Webhook, error) {
	var u, sec sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT callback_url, callback_secret FROM tenants WHERE id=?`, tenantID).Scan(&u, &sec)
	if errors.Is(err, sql.ErrNoRows) {
		return Webhook{}, ErrNotFound
	}
	return Webhook{URL: u.String, Secret: sec.String}, err
}

// SetWebhook 设置回调地址与签名密钥；url 为空表示关闭。
func (s *Store) SetWebhook(ctx context.Context, tenantID int64, url, secret string) error {
	if url == "" {
		_, err := s.db.ExecContext(ctx, `UPDATE tenants SET callback_url=NULL, callback_secret=NULL WHERE id=?`, tenantID)
		return err
	}
	_, err := s.db.ExecContext(ctx, `UPDATE tenants SET callback_url=?, callback_secret=? WHERE id=?`, url, secret, tenantID)
	return err
}

// ---- 回调事件发件箱 ----

type WebhookEvent struct {
	ID        int64
	TenantID  int64
	Type      string
	Payload   []byte
	Attempts  int
	CreatedAt time.Time
}

// EnqueueEvent 仅当租户配置了回调地址时才写入（单条 INSERT…SELECT，无额外查询）。
func (s *Store) EnqueueEvent(ctx context.Context, tenantID int64, typ string, payload []byte) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO webhook_events(tenant_id,event_type,payload,next_at)
SELECT id,?,?,NOW(3) FROM tenants WHERE id=? AND callback_url IS NOT NULL AND callback_url<>''`, typ, string(payload), tenantID)
	return err
}

// ClaimEvents 领取到期事件：FOR UPDATE SKIP LOCKED 让多实例互不重复，
// 同时把 next_at 推后 lease 作为租约——投递中崩溃的事件会在租约到期后被重新领取。
func (s *Store) ClaimEvents(ctx context.Context, limit int, lease time.Duration) ([]WebhookEvent, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id,tenant_id,event_type,payload,attempts,created_at FROM webhook_events
WHERE status='pending' AND next_at<=NOW(3) ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	var out []WebhookEvent
	for rows.Next() {
		var e WebhookEvent
		var p string
		if err := rows.Scan(&e.ID, &e.TenantID, &e.Type, &p, &e.Attempts, &e.CreatedAt); err != nil {
			rows.Close()
			return nil, err
		}
		e.Payload, e.Attempts = []byte(p), e.Attempts+1
		out = append(out, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, e := range out {
		if _, err := tx.ExecContext(ctx, `UPDATE webhook_events SET attempts=?, next_at=DATE_ADD(NOW(3), INTERVAL ? SECOND) WHERE id=?`, e.Attempts, int(lease.Seconds()), e.ID); err != nil {
			return nil, err
		}
	}
	return out, tx.Commit()
}

func (s *Store) FinishEvent(ctx context.Context, id int64, status, lastErr string) error {
	if len(lastErr) > 250 {
		lastErr = lastErr[:250]
	}
	_, err := s.db.ExecContext(ctx, `UPDATE webhook_events SET status=?, last_error=? WHERE id=?`, status, lastErr, id)
	return err
}

func (s *Store) RescheduleEvent(ctx context.Context, id int64, delay time.Duration, lastErr string) error {
	if len(lastErr) > 250 {
		lastErr = lastErr[:250]
	}
	_, err := s.db.ExecContext(ctx, `UPDATE webhook_events SET next_at=DATE_ADD(NOW(3), INTERVAL ? MICROSECOND), last_error=? WHERE id=?`, delay.Microseconds(), lastErr, id)
	return err
}

func (s *Store) PurgeEvents(ctx context.Context, olderThan time.Duration) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM webhook_events WHERE status<>'pending' AND created_at < DATE_SUB(NOW(3), INTERVAL ? SECOND)`, int(olderThan.Seconds()))
	return err
}

// ---- 最近会话 / 已读 ----

type RecentItem struct {
	Session
	Unread int `json:"unread"`
}

// MarkRead 只前进不后退。
func (s *Store) MarkRead(ctx context.Context, sessionID int64, accid string, seq int64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO read_cursors(session_id,accid,read_seq) VALUES(?,?,?)
ON DUPLICATE KEY UPDATE read_seq=GREATEST(read_seq, VALUES(read_seq)), updated_at=CURRENT_TIMESTAMP(3)`, sessionID, accid, seq)
	return err
}

// RecentSessions 返回账号参与的会话，按最后活动时间倒序，并带未读数。
func (s *Store) RecentSessions(ctx context.Context, tenantID int64, accid string, includeClosed bool, limit int) ([]RecentItem, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+sessColsS+`,
 (SELECT COUNT(*) FROM messages m WHERE m.session_id=s.id AND m.seq > COALESCE(rc.read_seq,0) AND m.from_accid<>? AND m.from_accid<>'system') AS unread
FROM sessions s LEFT JOIN read_cursors rc ON rc.session_id=s.id AND rc.accid=?
WHERE s.tenant_id=? AND (s.visitor_accid=? OR s.agent_accid=?) AND (? OR s.status<>'closed')
ORDER BY COALESCE(s.last_msg_at, s.created_at) DESC, s.id DESC LIMIT ?`,
		accid, accid, tenantID, accid, accid, includeClosed, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RecentItem
	for rows.Next() {
		var it RecentItem
		var closed, lastAt sql.NullTime
		x := &it.Session
		if err := rows.Scan(&x.ID, &x.TenantID, &x.VisitorAccid, &x.AgentAccid, &x.Status, &x.SkillGroup, &x.BotMisses, &x.LastSeq, &x.LastMsg, &lastAt, &x.CreatedAt, &closed, &it.Unread); err != nil {
			return nil, err
		}
		if closed.Valid {
			x.ClosedAt = &closed.Time
		}
		if lastAt.Valid {
			x.LastMsgAt = &lastAt.Time
		}
		out = append(out, it)
	}
	return out, rows.Err()
}
