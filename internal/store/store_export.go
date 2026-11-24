package store

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// ExportPageSize 导出时每批读取的会话数（键集分页，内存占用与总量无关）。
var ExportPageSize = 200

type ExportFilter struct {
	From, To time.Time // 会话创建时间 [From, To)
	Status   string
	Agent    string
	Visitor  string
	Limit    int // 最多输出的行数（会话导出=会话数，消息导出=消息数）
}

type ExportSession struct {
	Session
	Messages      int // 访客与坐席发送的消息数
	Transfers     int
	RatingScore   *int
	RatingComment string
}

func (f ExportFilter) args(tenantID int64, after int64, n int) []any {
	return []any{tenantID, f.From.UTC(), f.To.UTC(), f.Status, f.Status, f.Agent, f.Agent, f.Visitor, f.Visitor, after, n}
}

const exportWhere = ` WHERE s.tenant_id=? AND s.created_at>=? AND s.created_at<? AND (?='' OR s.status=?) AND (?='' OR s.agent_accid=?) AND (?='' OR s.visitor_accid=?) AND s.id>? ORDER BY s.id LIMIT ?`

func scanSessionCols(rows *sql.Rows, extra ...any) (*Session, error) {
	x := &Session{}
	var closed, lastAt sql.NullTime
	dest := append([]any{&x.ID, &x.TenantID, &x.VisitorAccid, &x.AgentAccid, &x.Status, &x.SkillGroup, &x.BotMisses, &x.LastSeq, &x.LastMsg, &lastAt, &x.CreatedAt, &closed}, extra...)
	if err := rows.Scan(dest...); err != nil {
		return nil, err
	}
	if closed.Valid {
		x.ClosedAt = &closed.Time
	}
	if lastAt.Valid {
		x.LastMsgAt = &lastAt.Time
	}
	return x, nil
}

func pageSize(f ExportFilter, emitted int) int {
	n := ExportPageSize
	if n < 1 {
		n = 1
	}
	if left := f.Limit - emitted; left < n {
		n = left
	}
	return n
}

// ExportSessions 按 ID 升序（即时间顺序）流式输出会话及其汇总信息；fn 返回错误则中止。
func (s *Store) ExportSessions(ctx context.Context, tenantID int64, f ExportFilter, fn func(*ExportSession) error) error {
	var after int64
	for emitted := 0; emitted < f.Limit; {
		rows, err := s.db.QueryContext(ctx, `SELECT `+sessColsS+`,
 (SELECT COUNT(*) FROM messages m WHERE m.session_id=s.id AND m.from_role IN ('visitor','agent')),
 (SELECT COUNT(*) FROM session_transfers t WHERE t.session_id=s.id),
 r.score, r.comment
FROM sessions s LEFT JOIN session_ratings r ON r.session_id=s.id`+exportWhere, f.args(tenantID, after, pageSize(f, emitted))...)
		if err != nil {
			return err
		}
		var batch []*ExportSession
		for rows.Next() {
			e := &ExportSession{}
			var score sql.NullInt64
			var comment sql.NullString
			sess, err := scanSessionCols(rows, &e.Messages, &e.Transfers, &score, &comment)
			if err != nil {
				rows.Close()
				return err
			}
			e.Session = *sess
			if score.Valid {
				v := int(score.Int64)
				e.RatingScore, e.RatingComment = &v, comment.String
			}
			batch = append(batch, e)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		for _, e := range batch {
			if err := fn(e); err != nil {
				return err
			}
			after = e.ID
			emitted++
		}
	}
	return nil
}

func (s *Store) exportSessionPage(ctx context.Context, tenantID int64, f ExportFilter, after int64, n int) ([]Session, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+sessColsS+` FROM sessions s`+exportWhere, f.args(tenantID, after, n)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Session
	for rows.Next() {
		x, err := scanSessionCols(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *x)
	}
	return out, rows.Err()
}

// ExportMessages 按会话 ID、消息 seq 升序流式输出符合条件的会话下的全部消息。
// 每批取 ExportPageSize 个会话，再一次性取这批会话的消息，内存占用有上界。
func (s *Store) ExportMessages(ctx context.Context, tenantID int64, f ExportFilter, excludeSystem bool, fn func(*Session, *Message) error) error {
	var after int64
	emitted := 0
	for emitted < f.Limit {
		page, err := s.exportSessionPage(ctx, tenantID, f, after, ExportPageSize)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			return nil
		}
		after = page[len(page)-1].ID
		bySess := make(map[int64]*Session, len(page))
		args := []any{tenantID}
		for i := range page {
			bySess[page[i].ID] = &page[i]
			args = append(args, page[i].ID)
		}
		q := `SELECT id,tenant_id,session_id,seq,client_msg_id,from_accid,from_role,type,body,created_at FROM messages WHERE tenant_id=? AND session_id IN (?` +
			strings.Repeat(",?", len(page)-1) + `)`
		if excludeSystem {
			q += ` AND from_role<>'system'`
		}
		rows, err := s.db.QueryContext(ctx, q+` ORDER BY session_id, seq`, args...)
		if err != nil {
			return err
		}
		var msgs []Message
		for rows.Next() {
			var m Message
			var c sql.NullString
			if err := rows.Scan(&m.ID, &m.TenantID, &m.SessionID, &m.Seq, &c, &m.From, &m.FromRole, &m.Type, &m.Body, &m.CreatedAt); err != nil {
				rows.Close()
				return err
			}
			m.ClientMsgID = c.String
			msgs = append(msgs, m)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for i := range msgs {
			if emitted >= f.Limit {
				return nil
			}
			if err := fn(bySess[msgs[i].SessionID], &msgs[i]); err != nil {
				return err
			}
			emitted++
		}
	}
	return nil
}
