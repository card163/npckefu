package api

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // 内置时区库：不依赖运行环境是否安装 tzdata

	"npckefu/internal/store"
)

const (
	exportDefaultLimit = 100_000
	exportMaxLimit     = 1_000_000
	exportTimeLayout   = "2006-01-02 15:04:05"
)

type exportParams struct {
	f             store.ExportFilter
	jsonl         bool
	loc           *time.Location
	excludeSystem bool
}

// 日期范围：from/to 为 RFC3339 或 YYYY-MM-DD（纯日期按 tz 时区的当天 0 点解释；to 为纯日期时包含当天）。默认最近 30 天。
func parseExport(q url.Values) (p exportParams, err error) {
	p.loc = time.UTC
	if tz := q.Get("tz"); tz != "" {
		if p.loc, err = time.LoadLocation(tz); err != nil {
			return p, errors.New("tz: unknown IANA time zone, e.g. Asia/Shanghai")
		}
	}
	switch q.Get("format") {
	case "", "csv":
	case "jsonl":
		p.jsonl = true
	default:
		return p, errors.New("format: csv or jsonl")
	}
	parse := func(name string, endOfDay bool) (time.Time, bool, error) {
		v := q.Get(name)
		if v == "" {
			return time.Time{}, false, nil
		}
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t, true, nil
		}
		t, err := time.ParseInLocation("2006-01-02", v, p.loc)
		if err != nil {
			return time.Time{}, false, fmt.Errorf("%s: RFC3339 or YYYY-MM-DD", name)
		}
		if endOfDay {
			t = t.AddDate(0, 0, 1)
		}
		return t, true, nil
	}
	to, ok, err := parse("to", true)
	if err != nil {
		return p, err
	}
	if !ok {
		to = time.Now().Add(time.Minute)
	}
	from, ok, err := parse("from", false)
	if err != nil {
		return p, err
	}
	if !ok {
		from = to.AddDate(0, 0, -30)
	}
	if !from.Before(to) {
		return p, errors.New("from must be before to")
	}
	p.f = store.ExportFilter{From: from, To: to, Status: q.Get("status"), Agent: q.Get("agent"), Visitor: q.Get("visitor"), Limit: exportDefaultLimit}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > exportMaxLimit {
			return p, fmt.Errorf("limit: 1..%d", exportMaxLimit)
		}
		p.f.Limit = n
	}
	switch p.f.Status {
	case "", "bot", "queued", "active", "closed":
	default:
		return p, errors.New("status: bot|queued|active|closed")
	}
	p.excludeSystem = q.Get("exclude_system") == "1"
	return p, nil
}

// safeCell 防 CSV 公式注入：访客可控的文本若以 = + - @ 或制表/回车开头，Excel/WPS 会把它当公式执行
// （例如 =HYPERLINK(...) 外泄数据）。前置一个单引号使其按普通文本显示。
func safeCell(s string) string {
	if s != "" && strings.ContainsRune("=+-@\t\r", rune(s[0])) {
		return "'" + s
	}
	return s
}

type exporter struct {
	w     http.ResponseWriter
	fl    http.Flusher
	csv   *csv.Writer
	enc   *json.Encoder
	jsonl bool
	n     int
}

func newExporter(w http.ResponseWriter, p exportParams, kind string) *exporter {
	name := fmt.Sprintf("%s-%s.%s", kind, time.Now().In(p.loc).Format("20060102-150405"), map[bool]string{false: "csv", true: "jsonl"}[p.jsonl])
	h := w.Header()
	h.Set("Content-Disposition", `attachment; filename="`+name+`"`)
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	e := &exporter{w: w, jsonl: p.jsonl}
	e.fl, _ = w.(http.Flusher)
	if p.jsonl {
		h.Set("Content-Type", "application/x-ndjson; charset=utf-8")
		e.enc = json.NewEncoder(w)
		e.enc.SetEscapeHTML(false)
	} else {
		h.Set("Content-Type", "text/csv; charset=utf-8")
		w.Write([]byte("\xEF\xBB\xBF")) // UTF-8 BOM：Excel 才会正确识别中文
		e.csv = csv.NewWriter(w)
	}
	return e
}

func (e *exporter) header(cols ...string) {
	if !e.jsonl {
		e.csv.Write(cols)
	}
}

func (e *exporter) row(cells []string, obj any) error {
	if e.jsonl {
		if err := e.enc.Encode(obj); err != nil {
			return err
		}
	} else if err := e.csv.Write(cells); err != nil {
		return err
	}
	if e.n++; e.n%200 == 0 {
		e.flush()
	}
	return nil
}

func (e *exporter) flush() {
	if e.csv != nil {
		e.csv.Flush()
	}
	if e.fl != nil {
		e.fl.Flush()
	}
}

func fmtTime(t time.Time, loc *time.Location) string { return t.In(loc).Format(exportTimeLayout) }

func fmtTimePtr(t *time.Time, loc *time.Location) string {
	if t == nil {
		return ""
	}
	return fmtTime(*t, loc)
}

// GET /v1/export/sessions —— 会话列表（每个会话一行，含评价、转接次数、消息数、时长）。
// 参数：from、to、status、agent、visitor、format=csv|jsonl、tz、limit。
func (a *API) exportSessions(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	p, err := parseExport(r.URL.Query())
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	log.Printf("export sessions tenant=%d %s..%s limit=%d", t.ID, p.f.From.Format(time.RFC3339), p.f.To.Format(time.RFC3339), p.f.Limit)
	e := newExporter(w, p, "sessions")
	e.header("session_id", "visitor_accid", "agent_accid", "status", "skill_group", "created_at", "closed_at", "duration_seconds", "messages", "transfers", "rating_score", "rating_comment")
	err = a.St.ExportSessions(r.Context(), t.ID, p.f, func(s *store.ExportSession) error {
		dur := ""
		if s.ClosedAt != nil {
			dur = strconv.Itoa(int(s.ClosedAt.Sub(s.CreatedAt).Seconds()))
		}
		score := ""
		if s.RatingScore != nil {
			score = strconv.Itoa(*s.RatingScore)
		}
		obj := map[string]any{
			"session_id": s.ID, "visitor_accid": s.VisitorAccid, "agent_accid": s.AgentAccid, "status": s.Status, "skill_group": s.SkillGroup,
			"created_at": s.CreatedAt.In(p.loc).Format(time.RFC3339), "closed_at": nil, "duration_seconds": nil,
			"messages": s.Messages, "transfers": s.Transfers, "rating_score": s.RatingScore, "rating_comment": s.RatingComment,
		}
		if s.ClosedAt != nil {
			obj["closed_at"], obj["duration_seconds"] = s.ClosedAt.In(p.loc).Format(time.RFC3339), int(s.ClosedAt.Sub(s.CreatedAt).Seconds())
		}
		return e.row([]string{strconv.FormatInt(s.ID, 10), safeCell(s.VisitorAccid), safeCell(s.AgentAccid), s.Status, safeCell(s.SkillGroup),
			fmtTime(s.CreatedAt, p.loc), fmtTimePtr(s.ClosedAt, p.loc), dur, strconv.Itoa(s.Messages), strconv.Itoa(s.Transfers), score, safeCell(s.RatingComment)}, obj)
	})
	exportDone(e, err, r.Context())
}

// GET /v1/export/messages —— 聊天记录（每条消息一行，按会话、顺序排列）。额外参数：exclude_system=1 不含系统消息/服务端通知。
// 图片消息的 content 为 [图片]，image_url 为 /files/<id>（拼上你的域名即可下载）。
func (a *API) exportMessages(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	p, err := parseExport(r.URL.Query())
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	log.Printf("export messages tenant=%d %s..%s limit=%d", t.ID, p.f.From.Format(time.RFC3339), p.f.To.Format(time.RFC3339), p.f.Limit)
	e := newExporter(w, p, "messages")
	e.header("session_id", "seq", "created_at", "from_role", "from_accid", "type", "content", "image_url", "visitor_accid", "agent_accid")
	err = a.St.ExportMessages(r.Context(), t.ID, p.f, p.excludeSystem, func(s *store.Session, m *store.Message) error {
		content, imgURL := m.Body, ""
		if m.Type == "image" {
			var i struct {
				URL string `json:"url"`
			}
			if json.Unmarshal([]byte(m.Body), &i) == nil {
				content, imgURL = "[图片]", i.URL
			}
		}
		obj := map[string]any{
			"session_id": m.SessionID, "seq": m.Seq, "created_at": m.CreatedAt.In(p.loc).Format(time.RFC3339), "from_role": m.FromRole, "from_accid": m.From,
			"type": m.Type, "content": content, "image_url": imgURL, "visitor_accid": s.VisitorAccid, "agent_accid": s.AgentAccid,
		}
		return e.row([]string{strconv.FormatInt(m.SessionID, 10), strconv.FormatInt(m.Seq, 10), fmtTime(m.CreatedAt, p.loc), m.FromRole, safeCell(m.From), m.Type,
			safeCell(content), imgURL, safeCell(s.VisitorAccid), safeCell(s.AgentAccid)}, obj)
	})
	exportDone(e, err, r.Context())
}

func exportDone(e *exporter, err error, ctx context.Context) {
	e.flush()
	if err != nil && ctx.Err() == nil { // 客户端中途断开不算服务端错误；响应已开始，只能记日志
		log.Printf("export aborted after %d rows: %v", e.n, err)
	}
}
