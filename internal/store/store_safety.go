package store

import "context"

// ---- 敏感词 ----

type SensitiveWord struct {
	ID     int64  `json:"id"`
	Word   string `json:"word"`
	Action string `json:"action"` // mask | block
}

const MaxSensitiveWords = 5000

func (s *Store) AddSensitiveWords(ctx context.Context, tenantID int64, words []string, action string) (added int64, err error) {
	for _, w := range words {
		r, err := s.db.ExecContext(ctx, `INSERT INTO sensitive_words(tenant_id,word,action) VALUES(?,?,?) ON DUPLICATE KEY UPDATE action=VALUES(action)`, tenantID, w, action)
		if err != nil {
			return added, err
		}
		n, _ := r.RowsAffected()
		if n > 0 {
			added++
		}
	}
	return added, nil
}

func (s *Store) CountSensitiveWords(ctx context.Context, tenantID int64) (n int, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sensitive_words WHERE tenant_id=?`, tenantID).Scan(&n)
	return
}

func (s *Store) ListSensitiveWords(ctx context.Context, tenantID int64) ([]SensitiveWord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,word,action FROM sensitive_words WHERE tenant_id=? ORDER BY id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SensitiveWord
	for rows.Next() {
		var w SensitiveWord
		if err := rows.Scan(&w.ID, &w.Word, &w.Action); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func (s *Store) DeleteSensitiveWord(ctx context.Context, tenantID, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sensitive_words WHERE tenant_id=? AND id=?`, tenantID, id)
	return err
}

// ---- 推送设备 ----

type Device struct {
	Platform string `json:"platform"`
	Token    string `json:"token"`
}

const maxDevicesPerAccount = 10

func (s *Store) UpsertDevice(ctx context.Context, tenantID int64, accid, platform, token string) error {
	if _, err := s.db.ExecContext(ctx, `INSERT INTO push_devices(tenant_id,token,accid,platform) VALUES(?,?,?,?)
ON DUPLICATE KEY UPDATE accid=VALUES(accid), platform=VALUES(platform), updated_at=CURRENT_TIMESTAMP(3)`, tenantID, token, accid, platform); err != nil {
		return err
	}
	// 每个账号只保留最近的 N 台设备
	_, err := s.db.ExecContext(ctx, `DELETE FROM push_devices WHERE tenant_id=? AND accid=? AND token NOT IN
(SELECT token FROM (SELECT token FROM push_devices WHERE tenant_id=? AND accid=? ORDER BY updated_at DESC, token LIMIT ?) t)`,
		tenantID, accid, tenantID, accid, maxDevicesPerAccount)
	return err
}

func (s *Store) DeleteDevice(ctx context.Context, tenantID int64, accid, token string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM push_devices WHERE tenant_id=? AND accid=? AND token=?`, tenantID, accid, token)
	return err
}

func (s *Store) DevicesOf(ctx context.Context, tenantID int64, accid string) ([]Device, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT platform,token FROM push_devices WHERE tenant_id=? AND accid=? ORDER BY updated_at DESC`, tenantID, accid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Device
	for rows.Next() {
		var d Device
		if err := rows.Scan(&d.Platform, &d.Token); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
