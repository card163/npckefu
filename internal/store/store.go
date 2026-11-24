// Package store 提供基于 MySQL 8（InnoDB, utf8mb4）的持久化层（多租户：所有表都带 tenant_id）。
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	RoleVisitor = "visitor"
	RoleAgent   = "agent"
	RoleBot     = "bot"
	RoleSystem  = "system"

	StatusBot    = "bot"    // 机器人接待中
	StatusQueued = "queued" // 等待人工
	StatusActive = "active" // 人工接待中
	StatusClosed = "closed"
)

var ErrNotFound = errors.New("not found")

type Tenant struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	AppKey    string `json:"app_key"`
	AppSecret string `json:"app_secret,omitempty"`
}

type Account struct {
	ID         int64  `json:"id"`
	TenantID   int64  `json:"tenant_id"`
	Accid      string `json:"accid"`
	Name       string `json:"name"`
	Role       string `json:"role"`
	Token      string `json:"token,omitempty"`
	SkillGroup string `json:"skill_group"`
	MaxLoad    int    `json:"max_load"`
}

type Session struct {
	ID           int64      `json:"id"`
	TenantID     int64      `json:"tenant_id"`
	VisitorAccid string     `json:"visitor_accid"`
	AgentAccid   string     `json:"agent_accid"`
	Status       string     `json:"status"`
	SkillGroup   string     `json:"skill_group"`
	BotMisses    int        `json:"bot_misses"`
	LastSeq      int64      `json:"last_seq"`
	LastMsg      string     `json:"last_msg"`
	LastMsgAt    *time.Time `json:"last_msg_at,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	ClosedAt     *time.Time `json:"closed_at,omitempty"`
}

type Message struct {
	ID          int64     `json:"id"`
	TenantID    int64     `json:"tenant_id"`
	SessionID   int64     `json:"session_id"`
	Seq         int64     `json:"seq"`
	ClientMsgID string    `json:"client_msg_id"`
	From        string    `json:"from"`
	FromRole    string    `json:"from_role"`
	Type        string    `json:"type"` // text|image|custom|system
	Body        string    `json:"body"`
	CreatedAt   time.Time `json:"created_at"`
}

type FAQ struct {
	ID       int64  `json:"id"`
	TenantID int64  `json:"tenant_id"`
	Question string `json:"question"`
	Answer   string `json:"answer"`
	Keywords string `json:"keywords"`
}

type Store struct{ db *sql.DB }

// 每条语句单独执行（MySQL 默认不允许多语句）。
var schema = []string{
	`CREATE TABLE IF NOT EXISTS tenants(
  id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(128) NOT NULL,
  app_key VARCHAR(32) NOT NULL, app_secret VARCHAR(64) NOT NULL,
  callback_url VARCHAR(512) NULL, callback_secret VARCHAR(64) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_app_key(app_key)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	`CREATE TABLE IF NOT EXISTS accounts(
  id BIGINT AUTO_INCREMENT PRIMARY KEY, tenant_id BIGINT NOT NULL,
  accid VARCHAR(64) NOT NULL, name VARCHAR(128) NOT NULL DEFAULT '', role VARCHAR(16) NOT NULL,
  token VARCHAR(64) NOT NULL, skill_group VARCHAR(64) NOT NULL DEFAULT '', max_load INT NOT NULL DEFAULT 5,
  last_seen_at DATETIME(3) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_tenant_accid(tenant_id, accid)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	`CREATE TABLE IF NOT EXISTS sessions(
  id BIGINT AUTO_INCREMENT PRIMARY KEY, tenant_id BIGINT NOT NULL,
  visitor_accid VARCHAR(64) NOT NULL, agent_accid VARCHAR(64) NOT NULL DEFAULT '',
  status VARCHAR(16) NOT NULL, skill_group VARCHAR(64) NOT NULL DEFAULT '',
  bot_misses INT NOT NULL DEFAULT 0, last_seq BIGINT NOT NULL DEFAULT 0,
  last_msg_at DATETIME(3) NULL, last_msg_body VARCHAR(255) NOT NULL DEFAULT '',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), closed_at DATETIME(3) NULL,
  KEY idx_visitor(tenant_id, visitor_accid, status),
  KEY idx_agent(tenant_id, agent_accid, status),
  KEY idx_status(tenant_id, status, id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	// client_msg_id 为空时存 NULL：唯一索引允许多个 NULL，非空时用于幂等去重。
	`CREATE TABLE IF NOT EXISTS messages(
  id BIGINT AUTO_INCREMENT PRIMARY KEY, tenant_id BIGINT NOT NULL,
  session_id BIGINT NOT NULL, seq BIGINT NOT NULL, client_msg_id VARCHAR(64) NULL,
  from_accid VARCHAR(64) NOT NULL, from_role VARCHAR(16) NOT NULL, type VARCHAR(16) NOT NULL, body TEXT NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_session_seq(session_id, seq),
  UNIQUE KEY uq_client(session_id, from_accid, client_msg_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	// 已读游标：未读数 = seq > read_seq 且非己方、非内部系统消息。
	`CREATE TABLE IF NOT EXISTS read_cursors(
  session_id BIGINT NOT NULL, accid VARCHAR(64) NOT NULL, read_seq BIGINT NOT NULL DEFAULT 0,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY(session_id, accid)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	// 回调事件发件箱：与业务写入同库持久化，由 webhook.Worker 异步投递并重试。
	`CREATE TABLE IF NOT EXISTS webhook_events(
  id BIGINT AUTO_INCREMENT PRIMARY KEY, tenant_id BIGINT NOT NULL,
  event_type VARCHAR(32) NOT NULL, payload MEDIUMTEXT NOT NULL,
  status VARCHAR(8) NOT NULL DEFAULT 'pending', attempts INT NOT NULL DEFAULT 0,
  next_at DATETIME(3) NOT NULL, last_error VARCHAR(255) NOT NULL DEFAULT '',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  KEY idx_due(status, next_at, id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	`CREATE TABLE IF NOT EXISTS sensitive_words(
  id BIGINT AUTO_INCREMENT PRIMARY KEY, tenant_id BIGINT NOT NULL,
  word VARCHAR(128) NOT NULL, action VARCHAR(8) NOT NULL DEFAULT 'mask',
  UNIQUE KEY uq_tenant_word(tenant_id, word)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	// 推送设备：一个 token 同一时刻只属于一个账号（同机换账号登录时转移）。
	`CREATE TABLE IF NOT EXISTS push_devices(
  tenant_id BIGINT NOT NULL, token VARCHAR(512) NOT NULL, accid VARCHAR(64) NOT NULL, platform VARCHAR(16) NOT NULL,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY(tenant_id, token), KEY idx_accid(tenant_id, accid)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	// 图片附件元数据；session_id 为空表示已上传但尚未被任何消息引用（超时未用会被清理）。
	`CREATE TABLE IF NOT EXISTS attachments(
  id CHAR(32) PRIMARY KEY, tenant_id BIGINT NOT NULL, uploader VARCHAR(64) NOT NULL,
  mime VARCHAR(32) NOT NULL, size INT NOT NULL, width INT NOT NULL, height INT NOT NULL,
  session_id BIGINT NULL, created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  KEY idx_uploader(tenant_id, uploader, created_at), KEY idx_orphan(session_id, created_at)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	// STORAGE=db 时的文件内容（fs 模式不使用）。
	`CREATE TABLE IF NOT EXISTS attachment_blobs(
  id CHAR(32) PRIMARY KEY, data MEDIUMBLOB NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	`CREATE TABLE IF NOT EXISTS faqs(
  id BIGINT AUTO_INCREMENT PRIMARY KEY, tenant_id BIGINT NOT NULL,
  question VARCHAR(512) NOT NULL, answer TEXT NOT NULL, keywords VARCHAR(512) NOT NULL DEFAULT '',
  KEY idx_tenant(tenant_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
}

// Open 连接 MySQL 并建表。dsn 形如 user:pass@tcp(127.0.0.1:3306)/npckefu（库需已存在）。
func Open(dsn string) (*Store, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid mysql dsn: %w", err)
	}
	cfg.ParseTime, cfg.Loc = true, time.UTC
	cfg.Params = map[string]string{"charset": "utf8mb4", "time_zone": "'+00:00'"} // NOW(3) 与 Go 侧 UTC 对齐
	cfg.Collation = "utf8mb4_unicode_ci"
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for { // 容器/数据库启动较慢时等待
		if err = db.PingContext(ctx); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("mysql unreachable: %w", err)
		case <-time.After(time.Second):
		}
	}
	for _, q := range schema {
		if _, err := db.Exec(q); err != nil {
			return nil, fmt.Errorf("migrate: %w", err)
		}
	}
	st := &Store{db: db}
	// 旧库升级：补列（CREATE TABLE IF NOT EXISTS 不会改已有表）。
	for _, c := range [][3]string{
		{"tenants", "callback_url", "VARCHAR(512) NULL"},
		{"tenants", "callback_secret", "VARCHAR(64) NULL"},
		{"sessions", "last_msg_at", "DATETIME(3) NULL"},
		{"sessions", "last_msg_body", "VARCHAR(255) NOT NULL DEFAULT ''"},
		{"accounts", "last_seen_at", "DATETIME(3) NULL"},
	} {
		if err := st.ensureColumn(ctx, c[0], c[1], c[2]); err != nil {
			return nil, fmt.Errorf("migrate: %w", err)
		}
	}
	return st, nil
}

func (s *Store) ensureColumn(ctx context.Context, table, col, ddl string) error {
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME=?`, table, col).Scan(&n); err != nil || n > 0 {
		return err
	}
	_, err := s.db.ExecContext(ctx, "ALTER TABLE "+table+" ADD COLUMN "+col+" "+ddl)
	return err
}

func (s *Store) Close() error { return s.db.Close() }

func RandHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ---- tenants ----

func (s *Store) CreateTenant(ctx context.Context, name string) (*Tenant, error) {
	t := &Tenant{Name: name, AppKey: RandHex(8), AppSecret: RandHex(16)}
	r, err := s.db.ExecContext(ctx, `INSERT INTO tenants(name,app_key,app_secret) VALUES(?,?,?)`, t.Name, t.AppKey, t.AppSecret)
	if err != nil {
		return nil, err
	}
	t.ID, _ = r.LastInsertId()
	return t, nil
}

func (s *Store) TenantByAppKey(ctx context.Context, key string) (*Tenant, error) {
	t := &Tenant{}
	err := s.db.QueryRowContext(ctx, `SELECT id,name,app_key,app_secret FROM tenants WHERE app_key=?`, key).
		Scan(&t.ID, &t.Name, &t.AppKey, &t.AppSecret)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return t, err
}

// ---- accounts ----

// UpsertAccount 创建账号；已存在则更新资料并返回现有 token（类似云信 accid 注册）。
func (s *Store) UpsertAccount(ctx context.Context, a *Account) (*Account, error) {
	if a.Token == "" {
		a.Token = RandHex(16)
	}
	if a.MaxLoad == 0 {
		a.MaxLoad = 5
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO accounts(tenant_id,accid,name,role,token,skill_group,max_load)
VALUES(?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE name=VALUES(name), skill_group=VALUES(skill_group), max_load=VALUES(max_load)`,
		a.TenantID, a.Accid, a.Name, a.Role, a.Token, a.SkillGroup, a.MaxLoad)
	if err != nil {
		return nil, err
	}
	return s.Account(ctx, a.TenantID, a.Accid)
}

func (s *Store) Account(ctx context.Context, tenantID int64, accid string) (*Account, error) {
	a := &Account{}
	err := s.db.QueryRowContext(ctx, `SELECT id,tenant_id,accid,name,role,token,skill_group,max_load FROM accounts WHERE tenant_id=? AND accid=?`, tenantID, accid).
		Scan(&a.ID, &a.TenantID, &a.Accid, &a.Name, &a.Role, &a.Token, &a.SkillGroup, &a.MaxLoad)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return a, err
}

func (s *Store) ListAccounts(ctx context.Context, tenantID int64, role string) ([]Account, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,tenant_id,accid,name,role,skill_group,max_load FROM accounts WHERE tenant_id=? AND (?='' OR role=?) ORDER BY id`, tenantID, role, role)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Account
	for rows.Next() {
		var a Account
		if err := rows.Scan(&a.ID, &a.TenantID, &a.Accid, &a.Name, &a.Role, &a.SkillGroup, &a.MaxLoad); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ---- sessions ----

const sessCols = `id,tenant_id,visitor_accid,agent_accid,status,skill_group,bot_misses,last_seq,last_msg_body,last_msg_at,created_at,closed_at`
const sessColsS = `s.id,s.tenant_id,s.visitor_accid,s.agent_accid,s.status,s.skill_group,s.bot_misses,s.last_seq,s.last_msg_body,s.last_msg_at,s.created_at,s.closed_at`

func scanSession(sc interface{ Scan(...any) error }) (*Session, error) {
	x := &Session{}
	var closed, lastAt sql.NullTime
	if err := sc.Scan(&x.ID, &x.TenantID, &x.VisitorAccid, &x.AgentAccid, &x.Status, &x.SkillGroup, &x.BotMisses, &x.LastSeq, &x.LastMsg, &lastAt, &x.CreatedAt, &closed); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
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

func (s *Store) Session(ctx context.Context, tenantID, id int64) (*Session, error) {
	return scanSession(s.db.QueryRowContext(ctx, `SELECT `+sessCols+` FROM sessions WHERE tenant_id=? AND id=?`, tenantID, id))
}

func (s *Store) OpenSessionOfVisitor(ctx context.Context, tenantID int64, visitor string) (*Session, error) {
	return scanSession(s.db.QueryRowContext(ctx, `SELECT `+sessCols+` FROM sessions WHERE tenant_id=? AND visitor_accid=? AND status!='closed' ORDER BY id DESC LIMIT 1`, tenantID, visitor))
}

func (s *Store) CreateSession(ctx context.Context, tenantID int64, visitor, skillGroup string) (*Session, error) {
	r, err := s.db.ExecContext(ctx, `INSERT INTO sessions(tenant_id,visitor_accid,status,skill_group) VALUES(?,?,?,?)`, tenantID, visitor, StatusBot, skillGroup)
	if err != nil {
		return nil, err
	}
	id, _ := r.LastInsertId()
	return s.Session(ctx, tenantID, id)
}

func (s *Store) ListSessions(ctx context.Context, tenantID int64, status, agent string, limit int) ([]Session, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+sessCols+` FROM sessions WHERE tenant_id=? AND (?='' OR status=?) AND (?='' OR agent_accid=?) ORDER BY id DESC LIMIT ?`,
		tenantID, status, status, agent, agent, limit)
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

func (s *Store) QueuedSessions(ctx context.Context, tenantID int64) ([]Session, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+sessCols+` FROM sessions WHERE tenant_id=? AND status='queued' ORDER BY id`, tenantID)
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

// ActiveLoad 返回各坐席当前接待中的会话数。
func (s *Store) ActiveLoad(ctx context.Context, tenantID int64) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT agent_accid,COUNT(*) FROM sessions WHERE tenant_id=? AND status='active' GROUP BY agent_accid`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := map[string]int{}
	for rows.Next() {
		var a string
		var n int
		if err := rows.Scan(&a, &n); err != nil {
			return nil, err
		}
		m[a] = n
	}
	return m, rows.Err()
}

func (s *Store) SetSessionStatus(ctx context.Context, tenantID, id int64, status, agent string) error {
	q := `UPDATE sessions SET status=?, agent_accid=? WHERE tenant_id=? AND id=?`
	if status == StatusClosed {
		q = `UPDATE sessions SET status=?, agent_accid=?, closed_at=CURRENT_TIMESTAMP WHERE tenant_id=? AND id=?`
	}
	_, err := s.db.ExecContext(ctx, q, status, agent, tenantID, id)
	return err
}

func (s *Store) IncBotMisses(ctx context.Context, id int64, reset bool) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	q := `UPDATE sessions SET bot_misses=bot_misses+1 WHERE id=?`
	if reset {
		q = `UPDATE sessions SET bot_misses=0 WHERE id=?`
	}
	if _, err := tx.ExecContext(ctx, q, id); err != nil {
		return 0, err
	}
	var n int
	if err := tx.QueryRowContext(ctx, `SELECT bot_misses FROM sessions WHERE id=?`, id).Scan(&n); err != nil {
		return 0, err
	}
	return n, tx.Commit()
}

// ---- messages ----

// AppendMessage 先对会话行加锁（SELECT … FOR UPDATE）以串行化同会话写入：
// 在锁内做幂等检查并分配会话内递增 seq，因此并发重发也不会产生重复消息或 seq 空洞。
func (s *Store) AppendMessage(ctx context.Context, m *Message) (*Message, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()
	var seq int64
	if err := tx.QueryRowContext(ctx, `SELECT last_seq FROM sessions WHERE id=? AND tenant_id=? FOR UPDATE`, m.SessionID, m.TenantID).Scan(&seq); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, ErrNotFound
		}
		return nil, false, err
	}
	var cid any // 空串存 NULL
	if m.ClientMsgID != "" {
		cid = m.ClientMsgID
		ex := &Message{}
		var c sql.NullString
		err := tx.QueryRowContext(ctx, `SELECT id,tenant_id,session_id,seq,client_msg_id,from_accid,from_role,type,body,created_at FROM messages WHERE session_id=? AND from_accid=? AND client_msg_id=?`,
			m.SessionID, m.From, m.ClientMsgID).Scan(&ex.ID, &ex.TenantID, &ex.SessionID, &ex.Seq, &c, &ex.From, &ex.FromRole, &ex.Type, &ex.Body, &ex.CreatedAt)
		if err == nil {
			ex.ClientMsgID = c.String
			return ex, true, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, false, err
		}
	}
	m.Seq = seq + 1
	m.CreatedAt = time.Now().UTC().Truncate(time.Millisecond)
	if _, err := tx.ExecContext(ctx, `UPDATE sessions SET last_seq=?, last_msg_at=?, last_msg_body=? WHERE id=?`, m.Seq, m.CreatedAt, preview(m), m.SessionID); err != nil {
		return nil, false, err
	}
	r, err := tx.ExecContext(ctx, `INSERT INTO messages(tenant_id,session_id,seq,client_msg_id,from_accid,from_role,type,body,created_at) VALUES(?,?,?,?,?,?,?,?,?)`,
		m.TenantID, m.SessionID, m.Seq, cid, m.From, m.FromRole, m.Type, m.Body, m.CreatedAt)
	if err != nil {
		return nil, false, err
	}
	m.ID, _ = r.LastInsertId()
	return m, false, tx.Commit()
}

// MessagesSince 拉取 seq > since 的消息（离线/断线重连同步）。
func (s *Store) MessagesSince(ctx context.Context, tenantID, sessionID, since int64, limit int) ([]Message, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,tenant_id,session_id,seq,client_msg_id,from_accid,from_role,type,body,created_at FROM messages WHERE tenant_id=? AND session_id=? AND seq>? ORDER BY seq LIMIT ?`,
		tenantID, sessionID, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Message
	for rows.Next() {
		var m Message
		var c sql.NullString
		if err := rows.Scan(&m.ID, &m.TenantID, &m.SessionID, &m.Seq, &c, &m.From, &m.FromRole, &m.Type, &m.Body, &m.CreatedAt); err != nil {
			return nil, err
		}
		m.ClientMsgID = c.String
		out = append(out, m)
	}
	return out, rows.Err()
}

// ---- faq ----

func (s *Store) AddFAQ(ctx context.Context, f *FAQ) error {
	r, err := s.db.ExecContext(ctx, `INSERT INTO faqs(tenant_id,question,answer,keywords) VALUES(?,?,?,?)`, f.TenantID, f.Question, f.Answer, f.Keywords)
	if err != nil {
		return err
	}
	f.ID, _ = r.LastInsertId()
	return nil
}

func (s *Store) DeleteFAQ(ctx context.Context, tenantID, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM faqs WHERE tenant_id=? AND id=?`, tenantID, id)
	return err
}

func (s *Store) ListFAQs(ctx context.Context, tenantID int64) ([]FAQ, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,tenant_id,question,answer,keywords FROM faqs WHERE tenant_id=? ORDER BY id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FAQ
	for rows.Next() {
		var f FAQ
		if err := rows.Scan(&f.ID, &f.TenantID, &f.Question, &f.Answer, &f.Keywords); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func preview(m *Message) string {
	if m.Type == "image" {
		return "[图片]"
	}
	r := []rune(m.Body)
	if len(r) > 100 {
		r = r[:100]
	}
	return string(r)
}
