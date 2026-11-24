package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type Rating struct {
	SessionID    int64     `json:"session_id"`
	VisitorAccid string    `json:"visitor_accid"`
	AgentAccid   string    `json:"agent_accid"` // 空表示仅机器人接待
	Score        int       `json:"score"`
	Comment      string    `json:"comment"`
	CreatedAt    time.Time `json:"created_at"`
}

// InsertRating 每个会话只能评价一次：已存在则返回 created=false，不覆盖。
func (s *Store) InsertRating(ctx context.Context, tenantID int64, r *Rating) (created bool, err error) {
	res, err := s.db.ExecContext(ctx, `INSERT IGNORE INTO session_ratings(session_id,tenant_id,visitor_accid,agent_accid,score,comment) VALUES(?,?,?,?,?,?)`,
		r.SessionID, tenantID, r.VisitorAccid, r.AgentAccid, r.Score, r.Comment)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) Rating(ctx context.Context, tenantID, sessionID int64) (*Rating, error) {
	r := &Rating{}
	err := s.db.QueryRowContext(ctx, `SELECT session_id,visitor_accid,agent_accid,score,comment,created_at FROM session_ratings WHERE tenant_id=? AND session_id=?`, tenantID, sessionID).
		Scan(&r.SessionID, &r.VisitorAccid, &r.AgentAccid, &r.Score, &r.Comment, &r.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return r, err
}

type RatingFilter struct {
	Agent    string // 空=不过滤
	MinScore int    // 0=不过滤
	MaxScore int
	BeforeID int64 // 翻页：只取 session_id < BeforeID
	Limit    int
}

func (s *Store) ListRatings(ctx context.Context, tenantID int64, f RatingFilter) ([]Rating, error) {
	if f.Limit <= 0 || f.Limit > 200 {
		f.Limit = 50
	}
	if f.MaxScore == 0 {
		f.MaxScore = 5
	}
	if f.BeforeID == 0 {
		f.BeforeID = 1<<62 - 1
	}
	rows, err := s.db.QueryContext(ctx, `SELECT session_id,visitor_accid,agent_accid,score,comment,created_at FROM session_ratings
WHERE tenant_id=? AND (?='' OR agent_accid=?) AND score>=? AND score<=? AND session_id<? ORDER BY session_id DESC LIMIT ?`,
		tenantID, f.Agent, f.Agent, f.MinScore, f.MaxScore, f.BeforeID, f.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Rating
	for rows.Next() {
		var r Rating
		if err := rows.Scan(&r.SessionID, &r.VisitorAccid, &r.AgentAccid, &r.Score, &r.Comment, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type AgentStat struct {
	AgentAccid string  `json:"agent_accid"` // 空表示机器人
	Count      int     `json:"count"`
	Avg        float64 `json:"avg"`
}

type RatingStats struct {
	Count            int            `json:"count"`
	Avg              float64        `json:"avg"`
	SatisfactionRate float64        `json:"satisfaction_rate"` // 4~5 分占比
	Distribution     map[string]int `json:"distribution"`      // "1".."5"
	ByAgent          []AgentStat    `json:"by_agent"`
}

func round2(f float64) float64 { return float64(int(f*100+0.5)) / 100 }

// Stats 统计 [from,to) 内的评价；agent 非空时只统计该坐席。
func (s *Store) RatingStats(ctx context.Context, tenantID int64, from, to time.Time, agent string) (*RatingStats, error) {
	st := &RatingStats{Distribution: map[string]int{"1": 0, "2": 0, "3": 0, "4": 0, "5": 0}, ByAgent: []AgentStat{}}
	rows, err := s.db.QueryContext(ctx, `SELECT score,COUNT(*) FROM session_ratings WHERE tenant_id=? AND created_at>=? AND created_at<? AND (?='' OR agent_accid=?) GROUP BY score`,
		tenantID, from.UTC(), to.UTC(), agent, agent)
	if err != nil {
		return nil, err
	}
	var sum, good int
	for rows.Next() {
		var score, n int
		if err := rows.Scan(&score, &n); err != nil {
			rows.Close()
			return nil, err
		}
		st.Distribution[string(rune('0'+score))] = n
		st.Count += n
		sum += score * n
		if score >= 4 {
			good += n
		}
	}
	rows.Close()
	if st.Count > 0 {
		st.Avg, st.SatisfactionRate = round2(float64(sum)/float64(st.Count)), round2(float64(good)/float64(st.Count))
	}
	rows, err = s.db.QueryContext(ctx, `SELECT agent_accid,COUNT(*),AVG(score) FROM session_ratings WHERE tenant_id=? AND created_at>=? AND created_at<? AND (?='' OR agent_accid=?) GROUP BY agent_accid ORDER BY COUNT(*) DESC, agent_accid`,
		tenantID, from.UTC(), to.UTC(), agent, agent)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var a AgentStat
		if err := rows.Scan(&a.AgentAccid, &a.Count, &a.Avg); err != nil {
			return nil, err
		}
		a.Avg = round2(a.Avg)
		st.ByAgent = append(st.ByAgent, a)
	}
	return st, rows.Err()
}
