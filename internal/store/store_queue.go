package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"unicode/utf8"
)

// MinRemindEvery 重复提醒的最小间隔（秒），避免把访客刷屏。测试里会调小。
var MinRemindEvery = 10

// QueueSettings 排队超时设置；秒数为 0 表示关闭对应功能。
type QueueSettings struct {
	RemindAfter   int    `json:"remind_after"`   // 排队多久后第一次提醒访客
	RemindEvery   int    `json:"remind_every"`   // 之后每隔多久再提醒
	RemindMax     int    `json:"remind_max"`     // 最多提醒几次
	AlertAfter    int    `json:"alert_after"`    // 排队多久视为"超时"，告警坐席/租户
	GiveupAfter   int    `json:"giveup_after"`   // 排队多久后放弃排队、退回机器人（0=不放弃）
	RemindMessage string `json:"remind_message"` // 可用 {position}（排队位置）和 {ahead}（前面还有几位）
	GiveupMessage string `json:"giveup_message"`
	IsDefault     bool   `json:"is_default"` // 该租户还没保存过设置，正在使用默认值
}

func DefaultQueueSettings() QueueSettings {
	return QueueSettings{
		RemindAfter: 60, RemindEvery: 60, RemindMax: 3, AlertAfter: 180, GiveupAfter: 0,
		RemindMessage: "目前咨询人数较多，您当前排在第 {position} 位，客服会尽快为您服务，请稍候。",
		GiveupMessage: "抱歉，目前客服繁忙，暂时没能为您接入人工，已为您转回智能助手。您可以继续提问，或稍后再输入“转人工”重新排队。",
		IsDefault:     true,
	}
}

// Validate 校验并补全空文案。
func (q *QueueSettings) Validate() error {
	for name, v := range map[string]int{"remind_after": q.RemindAfter, "remind_every": q.RemindEvery, "alert_after": q.AlertAfter, "giveup_after": q.GiveupAfter} {
		if v < 0 || v > 86400 {
			return fmt.Errorf("%s: 0..86400 seconds (0 = off)", name)
		}
	}
	if q.RemindMax < 0 || q.RemindMax > 20 {
		return errors.New("remind_max: 0..20")
	}
	if q.RemindAfter > 0 && q.RemindMax > 1 && q.RemindEvery < MinRemindEvery {
		return fmt.Errorf("remind_every: at least %d seconds when reminding more than once", MinRemindEvery)
	}
	d := DefaultQueueSettings()
	if q.RemindMessage == "" {
		q.RemindMessage = d.RemindMessage
	}
	if q.GiveupMessage == "" {
		q.GiveupMessage = d.GiveupMessage
	}
	if utf8.RuneCountInString(q.RemindMessage) > 200 || utf8.RuneCountInString(q.GiveupMessage) > 200 {
		return errors.New("messages: at most 200 characters")
	}
	return nil
}

func (s *Store) QueueSettings(ctx context.Context, tenantID int64) (QueueSettings, error) {
	var q QueueSettings
	err := s.db.QueryRowContext(ctx, `SELECT remind_after,remind_every,remind_max,alert_after,giveup_after,remind_message,giveup_message FROM queue_settings WHERE tenant_id=?`, tenantID).
		Scan(&q.RemindAfter, &q.RemindEvery, &q.RemindMax, &q.AlertAfter, &q.GiveupAfter, &q.RemindMessage, &q.GiveupMessage)
	if errors.Is(err, sql.ErrNoRows) {
		return DefaultQueueSettings(), nil
	}
	return q, err
}

func (s *Store) SaveQueueSettings(ctx context.Context, tenantID int64, q QueueSettings) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO queue_settings(tenant_id,remind_after,remind_every,remind_max,alert_after,giveup_after,remind_message,giveup_message) VALUES(?,?,?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE remind_after=VALUES(remind_after), remind_every=VALUES(remind_every), remind_max=VALUES(remind_max), alert_after=VALUES(alert_after),
 giveup_after=VALUES(giveup_after), remind_message=VALUES(remind_message), giveup_message=VALUES(giveup_message), updated_at=CURRENT_TIMESTAMP(3)`,
		tenantID, q.RemindAfter, q.RemindEvery, q.RemindMax, q.AlertAfter, q.GiveupAfter, q.RemindMessage, q.GiveupMessage)
	return err
}

// QueueWatchRow 排队中的会话及其已等待时间（等待秒数由数据库时钟计算，避免应用与数据库时钟不一致）。
type QueueWatchRow struct {
	Session
	WaitSecs  int
	Reminders int
	Alerted   bool
}

// QueuedForWatch 按排队先后返回排队中的会话。顺带给"升级前就在排队、还没有起点时间"的会话补上起点。
func (s *Store) QueuedForWatch(ctx context.Context, tenantID int64) ([]QueueWatchRow, error) {
	if _, err := s.db.ExecContext(ctx, `UPDATE sessions SET queued_at=NOW(3) WHERE tenant_id=? AND status='queued' AND queued_at IS NULL`, tenantID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+sessCols+`, TIMESTAMPDIFF(SECOND, queued_at, NOW(3)), queue_reminders, queue_alerted FROM sessions
WHERE tenant_id=? AND status='queued' AND queued_at IS NOT NULL ORDER BY queued_at, id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QueueWatchRow
	for rows.Next() {
		var r QueueWatchRow
		var alerted int
		x, err := scanSessionRow(rows, &r.WaitSecs, &r.Reminders, &alerted)
		if err != nil {
			return nil, err
		}
		r.Session, r.Alerted = *x, alerted != 0
		out = append(out, r)
	}
	return out, rows.Err()
}

// ClaimReminder 原子地"认领"第 n+1 次提醒：只有一个节点能成功，避免多节点重复提醒。
func (s *Store) ClaimReminder(ctx context.Context, tenantID, id int64, reminders int) (bool, error) {
	r, err := s.db.ExecContext(ctx, `UPDATE sessions SET queue_reminders=queue_reminders+1 WHERE id=? AND tenant_id=? AND status='queued' AND queue_reminders=?`, id, tenantID, reminders)
	if err != nil {
		return false, err
	}
	n, _ := r.RowsAffected()
	return n > 0, nil
}

func (s *Store) ClaimAlert(ctx context.Context, tenantID, id int64) (bool, error) {
	r, err := s.db.ExecContext(ctx, `UPDATE sessions SET queue_alerted=1 WHERE id=? AND tenant_id=? AND status='queued' AND queue_alerted=0`, id, tenantID)
	if err != nil {
		return false, err
	}
	n, _ := r.RowsAffected()
	return n > 0, nil
}

// ClaimGiveup 放弃排队：仍在排队才生效，退回机器人接待，并清掉该会话尚未被接手的转接标记。
func (s *Store) ClaimGiveup(ctx context.Context, tenantID, id int64) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	r, err := tx.ExecContext(ctx, `UPDATE sessions SET status='bot', agent_accid='', queued_at=NULL, queue_reminders=0, queue_alerted=0, bot_misses=0 WHERE id=? AND tenant_id=? AND status='queued'`, id, tenantID)
	if err != nil {
		return false, err
	}
	if n, _ := r.RowsAffected(); n == 0 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE session_transfers SET done=1 WHERE tenant_id=? AND session_id=? AND done=0`, tenantID, id); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// QueueStats 排队概况：人数、最长等待秒数、已超过 alertAfter 的人数（alertAfter<=0 时不统计超时）。
func (s *Store) QueueStats(ctx context.Context, tenantID int64, alertAfter int) (count, longest, overdue int, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MAX(TIMESTAMPDIFF(SECOND, queued_at, NOW(3))),0),
 COALESCE(SUM(?>0 AND TIMESTAMPDIFF(SECOND, queued_at, NOW(3)) >= ?),0) FROM sessions WHERE tenant_id=? AND status='queued' AND queued_at IS NOT NULL`,
		alertAfter, alertAfter, tenantID).Scan(&count, &longest, &overdue)
	return
}
