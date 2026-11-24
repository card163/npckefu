package store

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// Presence 是账号的持久化部分（最后在线时间）；是否在线由进程内连接表决定。
type PresenceRow struct {
	Accid      string
	Role       string
	LastSeenAt *time.Time
}

// TouchLastSeen 记录账号最后一次断开连接的时间。
func (s *Store) TouchLastSeen(ctx context.Context, tenantID int64, accid string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE accounts SET last_seen_at=NOW(3) WHERE tenant_id=? AND accid=?`, tenantID, accid)
	return err
}

// PresenceRows 批量读取账号角色与最后在线时间；不存在的账号不在结果里。
func (s *Store) PresenceRows(ctx context.Context, tenantID int64, accids []string) (map[string]PresenceRow, error) {
	out := map[string]PresenceRow{}
	if len(accids) == 0 {
		return out, nil
	}
	args := []any{tenantID}
	for _, a := range accids {
		args = append(args, a)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT accid,role,last_seen_at FROM accounts WHERE tenant_id=? AND accid IN (?`+strings.Repeat(",?", len(accids)-1)+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var r PresenceRow
		var t sql.NullTime
		if err := rows.Scan(&r.Accid, &r.Role, &t); err != nil {
			return nil, err
		}
		if t.Valid {
			r.LastSeenAt = &t.Time
		}
		out[r.Accid] = r
	}
	return out, rows.Err()
}
