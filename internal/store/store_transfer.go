package store

import (
	"context"
	"time"
)

type Transfer struct {
	ID         int64     `json:"id"`
	SessionID  int64     `json:"session_id"`
	FromAgent  string    `json:"from_agent"`
	ToAgent    string    `json:"to_agent"` // 空：退回队列/技能组且尚未被接手
	SkillGroup string    `json:"skill_group"`
	Note       string    `json:"note"`
	Done       bool      `json:"-"`
	CreatedAt  time.Time `json:"created_at"`
}

// ApplyTransfer 在一个事务里改派会话并写入转接记录。
// 改派带条件（会话仍是 active 且仍属于 FromAgent），因此与"结束会话/另一次转接"并发时只有一方成功，返回 ok=false。
// newAgent 非空：直接转给该坐席（保持 active）；为空：回到排队（queued，可同时设置技能组）。
func (s *Store) ApplyTransfer(ctx context.Context, tenantID int64, t *Transfer, newAgent, newSkillGroup string) (ok bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	status := StatusQueued
	if newAgent != "" {
		status = StatusActive
	}
	q := `UPDATE sessions SET agent_accid=?, status=? WHERE id=? AND tenant_id=? AND status='active' AND agent_accid=?`
	args := []any{newAgent, status, t.SessionID, tenantID, t.FromAgent}
	if newSkillGroup != "" {
		q = `UPDATE sessions SET agent_accid=?, status=?, skill_group=? WHERE id=? AND tenant_id=? AND status='active' AND agent_accid=?`
		args = []any{newAgent, status, newSkillGroup, t.SessionID, tenantID, t.FromAgent}
	}
	r, err := tx.ExecContext(ctx, q, args...)
	if err != nil {
		return false, err
	}
	if n, _ := r.RowsAffected(); n == 0 {
		return false, nil
	}
	done := 0
	if newAgent != "" {
		done = 1
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO session_transfers(tenant_id,session_id,from_agent,to_agent,skill_group,note,done) VALUES(?,?,?,?,?,?,?)`,
		tenantID, t.SessionID, t.FromAgent, newAgent, newSkillGroup, t.Note, done); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// PendingTransfers 返回当前"已退回队列、尚未被接手"的转接（按会话 ID 索引），供分配时排除原坐席并把备注交给接手人。
func (s *Store) PendingTransfers(ctx context.Context, tenantID int64) (map[int64]Transfer, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,session_id,from_agent,skill_group,note,created_at FROM session_transfers WHERE tenant_id=? AND done=0 ORDER BY id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]Transfer{}
	for rows.Next() {
		var t Transfer
		if err := rows.Scan(&t.ID, &t.SessionID, &t.FromAgent, &t.SkillGroup, &t.Note, &t.CreatedAt); err != nil {
			return nil, err
		}
		out[t.SessionID] = t // 同一会话有多条时取最新
	}
	return out, rows.Err()
}

// CompleteTransfer 排队转接被某坐席接手后，记录接手人并清除待处理标记。
func (s *Store) CompleteTransfer(ctx context.Context, tenantID, sessionID int64, toAgent string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE session_transfers SET done=1, to_agent=? WHERE tenant_id=? AND session_id=? AND done=0`, toAgent, tenantID, sessionID)
	return err
}

func (s *Store) ListTransfers(ctx context.Context, tenantID, sessionID int64) ([]Transfer, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,session_id,from_agent,to_agent,skill_group,note,done,created_at FROM session_transfers WHERE tenant_id=? AND session_id=? ORDER BY id`, tenantID, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Transfer
	for rows.Next() {
		var t Transfer
		if err := rows.Scan(&t.ID, &t.SessionID, &t.FromAgent, &t.ToAgent, &t.SkillGroup, &t.Note, &t.Done, &t.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) CountTransfers(ctx context.Context, tenantID, sessionID int64) (n int, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM session_transfers WHERE tenant_id=? AND session_id=?`, tenantID, sessionID).Scan(&n)
	return
}

// AgentGroupExists 该租户下是否至少有一个坐席属于此技能组（不论是否在线）。
func (s *Store) AgentGroupExists(ctx context.Context, tenantID int64, group string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM accounts WHERE tenant_id=? AND role='agent' AND skill_group=?`, tenantID, group).Scan(&n)
	return n > 0, err
}
