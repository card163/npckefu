package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type Attachment struct {
	ID        string    `json:"file_id"`
	TenantID  int64     `json:"-"`
	Uploader  string    `json:"-"`
	Mime      string    `json:"mime"`
	Size      int       `json:"size"`
	Width     int       `json:"width"`
	Height    int       `json:"height"`
	SessionID *int64    `json:"-"`
	CreatedAt time.Time `json:"-"`
}

const attCols = `id,tenant_id,uploader,mime,size,width,height,session_id,created_at`

func scanAtt(sc interface{ Scan(...any) error }) (*Attachment, error) {
	a := &Attachment{}
	var sid sql.NullInt64
	if err := sc.Scan(&a.ID, &a.TenantID, &a.Uploader, &a.Mime, &a.Size, &a.Width, &a.Height, &sid, &a.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if sid.Valid {
		a.SessionID = &sid.Int64
	}
	return a, nil
}

func (s *Store) InsertAttachment(ctx context.Context, a *Attachment) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO attachments(id,tenant_id,uploader,mime,size,width,height) VALUES(?,?,?,?,?,?,?)`,
		a.ID, a.TenantID, a.Uploader, a.Mime, a.Size, a.Width, a.Height)
	return err
}

func (s *Store) Attachment(ctx context.Context, id string) (*Attachment, error) {
	return scanAtt(s.db.QueryRowContext(ctx, `SELECT `+attCols+` FROM attachments WHERE id=?`, id))
}

// CountRecentUploads 统计某上传者最近 window 内的上传次数（限流用，多实例下天然一致）。
func (s *Store) CountRecentUploads(ctx context.Context, tenantID int64, uploader string, window time.Duration) (n int, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM attachments WHERE tenant_id=? AND uploader=? AND created_at > DATE_SUB(NOW(3), INTERVAL ? SECOND)`,
		tenantID, uploader, int(window.Seconds())).Scan(&n)
	return
}

// BindAttachment 把"本人上传、且未绑定或已绑定在同一会话"的附件绑定到会话并返回它；否则返回 ErrNotFound。
// 这保证了图片消息只能引用自己上传的文件，不能引用他人/其他租户的文件或任意外链。
func (s *Store) BindAttachment(ctx context.Context, tenantID int64, uploader, id string, sessionID int64) (*Attachment, error) {
	r, err := s.db.ExecContext(ctx, `UPDATE attachments SET session_id=? WHERE id=? AND tenant_id=? AND uploader=? AND (session_id IS NULL OR session_id=?)`,
		sessionID, id, tenantID, uploader, sessionID)
	if err != nil {
		return nil, err
	}
	if n, _ := r.RowsAffected(); n == 0 {
		// 已绑定同一会话时 RowsAffected 也可能为 0（值未变），再查一次确认归属
		a, err := s.Attachment(ctx, id)
		if err != nil || a.TenantID != tenantID || a.Uploader != uploader || a.SessionID == nil || *a.SessionID != sessionID {
			return nil, ErrNotFound
		}
		return a, nil
	}
	return s.Attachment(ctx, id)
}

// OrphanAttachments 返回超过 olderThan 仍未被任何消息引用的附件。
func (s *Store) OrphanAttachments(ctx context.Context, olderThan time.Duration, limit int) ([]Attachment, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+attCols+` FROM attachments WHERE session_id IS NULL AND created_at < DATE_SUB(NOW(3), INTERVAL ? SECOND) ORDER BY created_at LIMIT ?`,
		int(olderThan.Seconds()), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Attachment
	for rows.Next() {
		a, err := scanAtt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

func (s *Store) DeleteAttachment(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM attachments WHERE id=?`, id)
	return err
}

// ---- 文件内容（STORAGE=db）----

func (s *Store) PutBlob(ctx context.Context, id string, data []byte) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO attachment_blobs(id,data) VALUES(?,?) ON DUPLICATE KEY UPDATE data=VALUES(data)`, id, data)
	return err
}

func (s *Store) GetBlob(ctx context.Context, id string) ([]byte, error) {
	var b []byte
	err := s.db.QueryRowContext(ctx, `SELECT data FROM attachment_blobs WHERE id=?`, id).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return b, err
}

func (s *Store) DeleteBlob(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM attachment_blobs WHERE id=?`, id)
	return err
}
