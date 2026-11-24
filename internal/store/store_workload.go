package store

import (
	"context"
	"database/sql"
	"sort"
	"time"
)

// ---- 接待记录的写入 ----

// InsertAssignment 记录"会话分给某坐席"。source: dispatch（排队分配）或 transfer（坐席/主管转接）。
// 防御：先结束该会话遗留的未结束接待，保证同一会话同一时刻最多一条进行中的接待。
func (s *Store) InsertAssignment(ctx context.Context, tenantID, sessionID int64, agent, source string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE session_assignments SET ended_at=NOW(3), end_reason='replaced' WHERE session_id=? AND ended_at IS NULL`, sessionID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO session_assignments(tenant_id,session_id,agent_accid,source,assigned_at) VALUES(?,?,?,?,NOW(3))`, tenantID, sessionID, agent, source); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) endAssignment(ctx context.Context, sessionID int64, reason string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE session_assignments SET ended_at=NOW(3), end_reason=? WHERE session_id=? AND ended_at IS NULL`, reason, sessionID)
	return err
}

// MarkFirstReply 坐席在当前接待中的第一条回复：只记一次（first_reply_at 仍为空时才写）。
func (s *Store) MarkFirstReply(ctx context.Context, sessionID int64, agent string, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE session_assignments SET first_reply_at=? WHERE session_id=? AND agent_accid=? AND ended_at IS NULL AND first_reply_at IS NULL`, at.UTC(), sessionID, agent)
	return err
}

// ---- 统计 ----

type AgentWorkload struct {
	AgentAccid       string   `json:"agent_accid"`
	Sessions         int      `json:"sessions"`                // 接待过的会话数（去重）
	Assignments      int      `json:"assignments"`             // 接待次数（同一会话被转回来会算多次）
	Replied          int      `json:"replied"`                 // 其中坐席有过回复的次数
	AvgFirstReply    *float64 `json:"avg_first_reply_seconds"` // 平均首次响应：从接到会话到第一条回复
	MaxFirstReply    *float64 `json:"max_first_reply_seconds"`
	Ended            int      `json:"ended"`              // 已结束的接待次数
	AvgHandle        *float64 `json:"avg_handle_seconds"` // 已结束接待的平均时长
	Messages         int      `json:"messages"`           // 发送的消息数
	TransferredIn    int      `json:"transferred_in"`     // 别人转给他的次数
	TransferredOut   int      `json:"transferred_out"`    // 他转给别人（或退回队列）的次数
	Ratings          int      `json:"ratings"`
	AvgRating        *float64 `json:"avg_rating"`
	SatisfactionRate *float64 `json:"satisfaction_rate"` // 4~5 分占比
	ratingsGood      int
}

func nf(n sql.NullFloat64) *float64 {
	if !n.Valid {
		return nil
	}
	v := float64(int(n.Float64*10+0.5)) / 10
	return &v
}

// AgentWorkloads 区间 [from,to) 内各坐席的工作量。接待按"接到会话的时间"归属；消息与评价按各自发生时间。
// agent 非空只统计该坐席。结果按接待会话数降序。
func (s *Store) AgentWorkloads(ctx context.Context, tenantID int64, from, to time.Time, agent string) ([]AgentWorkload, error) {
	m := map[string]*AgentWorkload{}
	get := func(a string) *AgentWorkload {
		if m[a] == nil {
			m[a] = &AgentWorkload{AgentAccid: a}
		}
		return m[a]
	}
	rows, err := s.db.QueryContext(ctx, `SELECT agent_accid, COUNT(DISTINCT session_id), COUNT(*), COALESCE(SUM(first_reply_at IS NOT NULL),0),
 AVG(CASE WHEN first_reply_at IS NOT NULL THEN TIMESTAMPDIFF(MICROSECOND, assigned_at, first_reply_at) END)/1000000,
 MAX(CASE WHEN first_reply_at IS NOT NULL THEN TIMESTAMPDIFF(MICROSECOND, assigned_at, first_reply_at) END)/1000000,
 COALESCE(SUM(ended_at IS NOT NULL),0),
 AVG(CASE WHEN ended_at IS NOT NULL THEN TIMESTAMPDIFF(MICROSECOND, assigned_at, ended_at) END)/1000000,
 COALESCE(SUM(end_reason='transferred'),0), COALESCE(SUM(source='transfer'),0)
FROM session_assignments WHERE tenant_id=? AND assigned_at>=? AND assigned_at<? AND (?='' OR agent_accid=?) GROUP BY agent_accid`,
		tenantID, from.UTC(), to.UTC(), agent, agent)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var a string
		var w AgentWorkload
		var avgF, maxF, avgH sql.NullFloat64
		if err := rows.Scan(&a, &w.Sessions, &w.Assignments, &w.Replied, &avgF, &maxF, &w.Ended, &avgH, &w.TransferredOut, &w.TransferredIn); err != nil {
			rows.Close()
			return nil, err
		}
		w.AgentAccid, w.AvgFirstReply, w.MaxFirstReply, w.AvgHandle = a, nf(avgF), nf(maxF), nf(avgH)
		*get(a) = w
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = s.db.QueryContext(ctx, `SELECT from_accid, COUNT(*) FROM messages WHERE tenant_id=? AND created_at>=? AND created_at<? AND from_role='agent' AND (?='' OR from_accid=?) GROUP BY from_accid`,
		tenantID, from.UTC(), to.UTC(), agent, agent)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var a string
		var n int
		if err := rows.Scan(&a, &n); err != nil {
			rows.Close()
			return nil, err
		}
		get(a).Messages = n
	}
	rows.Close()

	rows, err = s.db.QueryContext(ctx, `SELECT agent_accid, COUNT(*), AVG(score), COALESCE(SUM(score>=4),0) FROM session_ratings WHERE tenant_id=? AND created_at>=? AND created_at<? AND agent_accid<>'' AND (?='' OR agent_accid=?) GROUP BY agent_accid`,
		tenantID, from.UTC(), to.UTC(), agent, agent)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var a string
		var n, good int
		var avg sql.NullFloat64
		if err := rows.Scan(&a, &n, &avg, &good); err != nil {
			rows.Close()
			return nil, err
		}
		w := get(a)
		w.Ratings, w.ratingsGood, w.AvgRating = n, good, nf(avg)
		if n > 0 {
			r := float64(int(float64(good)/float64(n)*1000+0.5)) / 1000
			w.SatisfactionRate = &r
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]AgentWorkload, 0, len(m))
	for _, w := range m {
		out = append(out, *w)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Sessions != out[j].Sessions {
			return out[i].Sessions > out[j].Sessions
		}
		return out[i].AgentAccid < out[j].AgentAccid
	})
	return out, nil
}

// WorkloadTotals 全体（或指定坐席）的去重会话数与接待次数。
func (s *Store) WorkloadTotals(ctx context.Context, tenantID int64, from, to time.Time, agent string) (sessions, assignments int, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT session_id), COUNT(*) FROM session_assignments WHERE tenant_id=? AND assigned_at>=? AND assigned_at<? AND (?='' OR agent_accid=?)`,
		tenantID, from.UTC(), to.UTC(), agent, agent).Scan(&sessions, &assignments)
	return
}

type DayStat struct {
	Date     string `json:"date"`
	Sessions int    `json:"sessions"` // 当天接待的会话数（去重）
	Messages int    `json:"messages"` // 当天坐席发送的消息数
}

// WorkloadDaily 按天汇总。offset 形如 "+08:00"，把 UTC 时间换算成统计所用时区后按日期分组
// （用固定偏移，不依赖 MySQL 的时区表；跨夏令时切换的区间，切换日前后会有一小时误差）。只返回有数据的日期。
func (s *Store) WorkloadDaily(ctx context.Context, tenantID int64, from, to time.Time, agent, offset string) (map[string]*DayStat, error) {
	out := map[string]*DayStat{}
	get := func(d string) *DayStat {
		if out[d] == nil {
			out[d] = &DayStat{Date: d}
		}
		return out[d]
	}
	rows, err := s.db.QueryContext(ctx, `SELECT DATE_FORMAT(CONVERT_TZ(assigned_at,'+00:00',?),'%Y-%m-%d') d, COUNT(DISTINCT session_id) FROM session_assignments
WHERE tenant_id=? AND assigned_at>=? AND assigned_at<? AND (?='' OR agent_accid=?) GROUP BY d`, offset, tenantID, from.UTC(), to.UTC(), agent, agent)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var d string
		var n int
		if err := rows.Scan(&d, &n); err != nil {
			rows.Close()
			return nil, err
		}
		get(d).Sessions = n
	}
	rows.Close()
	rows, err = s.db.QueryContext(ctx, `SELECT DATE_FORMAT(CONVERT_TZ(created_at,'+00:00',?),'%Y-%m-%d') d, COUNT(*) FROM messages
WHERE tenant_id=? AND created_at>=? AND created_at<? AND from_role='agent' AND (?='' OR from_accid=?) GROUP BY d`, offset, tenantID, from.UTC(), to.UTC(), agent, agent)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var d string
		var n int
		if err := rows.Scan(&d, &n); err != nil {
			rows.Close()
			return nil, err
		}
		get(d).Messages = n
	}
	rows.Close()
	return out, rows.Err()
}
