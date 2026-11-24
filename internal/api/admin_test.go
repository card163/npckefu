package api_test

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"npckefu/internal/im"
	"npckefu/internal/store"
)

func TestAdminAPI(t *testing.T) {
	e := newEnv(t)
	base := e.srv.URL
	get := func(path string, out any) int { return do(t, "GET", base+path, e.h, nil, out) }

	// ---- 登录校验：凭证对/错 ----
	var info map[string]any
	if c := get("/v1/tenant", &info); c != 200 || info["app_key"] != e.tenant.AppKey || info["name"] != "safety" || info["app_secret"] != nil {
		t.Fatalf("tenant info: %d %v", c, info)
	}
	if c := do(t, "GET", base+"/v1/tenant", map[string]string{"X-App-Key": e.tenant.AppKey, "X-App-Secret": "bad"}, nil, nil); c != 401 {
		t.Fatalf("bad secret: %d", c)
	}

	// ---- 准备：1 个坐席（上限 1）在线；v1 机器人、v2 人工接待、v3 排队 ----
	ag := mkAgent(t, e, "adm-ag", "vip", 1)
	ac := dial(t, base, e.tenant.AppKey, ag)
	defer ac.Close()
	time.Sleep(200 * time.Millisecond)
	v1 := e.visitor(t)
	c1 := dial(t, base, e.tenant.AppKey, v1)
	defer c1.Close()
	c1.WriteJSON(map[string]any{"op": "start"})
	s1 := wait(t, c1, func(x im.Event) bool { return x.Op == "session" }).Session
	c2, v2, s2 := openHuman(t, e, ac)
	defer c2.Close()
	v3 := e.visitor(t)
	c3 := dial(t, base, e.tenant.AppKey, v3)
	defer c3.Close()
	c3.WriteJSON(map[string]any{"op": "start"})
	s3 := wait(t, c3, func(x im.Event) bool { return x.Op == "session" }).Session
	c3.WriteJSON(map[string]any{"op": "send", "session_id": s3.ID, "client_msg_id": "q", "body": "转人工"})
	wait(t, c3, func(x im.Event) bool { return x.Op == "queue" })

	// ---- 单个会话 / 账号参数校验 ----
	var one store.Session
	if c := get(fmt.Sprintf("/v1/sessions/%d", s3.ID), &one); c != 200 || one.ID != s3.ID || one.VisitorAccid != v3.Accid {
		t.Fatalf("get session: %d %+v", c, one)
	}
	if c := get("/v1/sessions/99999999", nil); c != 404 {
		t.Fatalf("get unknown session: %d", c)
	}
	for _, bad := range []store.Account{
		{Accid: "has space", Role: "agent"}, {Accid: "a/b", Role: "agent"}, {Accid: "", Role: "agent"},
		{Accid: strings.Repeat("x", 65), Role: "agent"}, {Accid: "ok", Role: "agent", MaxLoad: 1001}, {Accid: "ok", Role: "admin"},
		{Accid: "ok", Role: "agent", SkillGroup: strings.Repeat("g", 65)},
	} {
		if c := do(t, "POST", base+"/v1/accounts", e.h, bad, nil); c != 400 {
			t.Errorf("account %+v: got %d, want 400", bad, c)
		}
	}
	if c := do(t, "POST", base+"/v1/accounts", e.h, store.Account{Accid: "a.b_c-d@e", Name: "ok", Role: "visitor"}, nil); c != 200 {
		t.Errorf("valid accid rejected: %d", c)
	}

	// ---- 概览 ----
	var ov struct {
		Live     map[string]int
		Agents   map[string]int
		Sessions map[string]int
		Messages int
		Rating   map[string]any
	}
	if c := get("/v1/stats/overview", &ov); c != 200 {
		t.Fatalf("overview: %d", c)
	}
	if ov.Live["bot"] != 1 || ov.Live["queued"] != 1 || ov.Live["active"] != 1 || ov.Agents["total"] != 1 || ov.Agents["online"] != 1 ||
		ov.Sessions["total"] != 3 || ov.Sessions["human"] != 1 || ov.Messages != 2 || ov.Rating["count"] != float64(0) {
		t.Fatalf("overview: %+v", ov)
	}
	if c := get("/v1/stats/overview?since=2999-01-01", &ov); c != 200 || ov.Sessions["total"] != 0 || ov.Messages != 0 || ov.Live["queued"] != 1 {
		t.Fatalf("overview since future: %d %+v", c, ov) // 区间统计清零，实时数不受影响
	}
	if c := get("/v1/stats/overview?since=yesterday", nil); c != 400 {
		t.Fatalf("bad since: %d", c)
	}

	// ---- 名单 ----
	var roster []im.RosterItem
	get("/v1/agents/roster", &roster)
	if len(roster) != 1 || !roster[0].Online || roster[0].Load != 1 || roster[0].MaxLoad != 1 || roster[0].SkillGroup != "vip" {
		t.Fatalf("roster: %+v", roster)
	}

	// ---- 会话检索：状态 / 坐席 / 访客 / 翻页 ----
	var l []store.Session
	get("/v1/sessions?status=queued", &l)
	if len(l) != 1 || l[0].ID != s3.ID {
		t.Fatalf("queued: %+v", l)
	}
	get("/v1/sessions?agent=adm-ag", &l)
	if len(l) != 1 || l[0].ID != s2.ID {
		t.Fatalf("by agent: %+v", l)
	}
	get("/v1/sessions?visitor="+v1.Accid, &l)
	if len(l) != 1 || l[0].ID != s1.ID || l[0].LastMsg == "" {
		t.Fatalf("by visitor: %+v", l)
	}
	get("/v1/sessions?limit=2", &l)
	if len(l) != 2 || l[0].ID != s3.ID || l[1].ID != s2.ID {
		t.Fatalf("page1: %+v", l)
	}
	get(fmt.Sprintf("/v1/sessions?limit=2&before=%d", l[1].ID), &l)
	if len(l) != 1 || l[0].ID != s1.ID {
		t.Fatalf("page2: %+v", l)
	}
	_ = v2

	// ---- 强制结束：访客与坐席收到通知；重复结束幂等；未知 404 ----
	if c := do(t, "POST", fmt.Sprintf("%s/v1/sessions/%d/close", base, s2.ID), e.h, nil, nil); c != 204 {
		t.Fatalf("close: %d", c)
	}
	wait(t, c2, func(x im.Event) bool { return x.Op == "session" && x.Session.Status == "closed" })
	wait(t, ac, func(x im.Event) bool {
		return x.Op == "session" && x.Session.ID == s2.ID && x.Session.Status == "closed"
	})
	if c := do(t, "POST", fmt.Sprintf("%s/v1/sessions/%d/close", base, s2.ID), e.h, nil, nil); c != 204 {
		t.Fatalf("close twice: %d", c)
	}
	if c := do(t, "POST", base+"/v1/sessions/99999999/close", e.h, nil, nil); c != 404 {
		t.Fatalf("close unknown: %d", c)
	}
	// 坐席空出来后，排队的会话被分配
	wait(t, ac, func(x im.Event) bool {
		return x.Op == "session" && x.Session.ID == s3.ID && x.Session.Status == "active"
	})

	// ---- FAQ 更新 ----
	var f store.FAQ
	do(t, "POST", base+"/v1/faqs", e.h, store.FAQ{Question: "怎么开发票", Answer: "旧答案", Keywords: "发票"}, &f)
	var up store.FAQ
	if c := do(t, "PUT", fmt.Sprintf("%s/v1/faqs/%d", base, f.ID), e.h, store.FAQ{Question: "如何开发票", Answer: "新答案", Keywords: "发票,开票"}, &up); c != 200 || up.Answer != "新答案" {
		t.Fatalf("update faq: %d %+v", c, up)
	}
	var faqs []store.FAQ
	get("/v1/faqs", &faqs)
	if len(faqs) != 1 || faqs[0].Question != "如何开发票" || faqs[0].Keywords != "发票,开票" {
		t.Fatalf("faqs: %+v", faqs)
	}
	if c := do(t, "PUT", fmt.Sprintf("%s/v1/faqs/%d", base, f.ID), e.h, store.FAQ{Question: "如何开发票", Answer: "新答案", Keywords: "发票,开票"}, nil); c != 200 {
		t.Fatalf("update with identical content must still be 200, got %d", c)
	}
	if c := do(t, "PUT", base+"/v1/faqs/99999999", e.h, store.FAQ{Question: "q", Answer: "a"}, nil); c != 404 {
		t.Fatalf("update unknown faq: %d", c)
	}
	if c := do(t, "PUT", fmt.Sprintf("%s/v1/faqs/%d", base, f.ID), e.h, store.FAQ{Question: "", Answer: "a"}, nil); c != 400 {
		t.Fatalf("update empty: %d", c)
	}

	// ---- 重置令牌：旧连接被踢，旧令牌失效，新令牌可用 ----
	var tr map[string]string
	if c := do(t, "POST", base+"/v1/accounts/adm-ag/token", e.h, nil, &tr); c != 200 || tr["token"] == "" || tr["token"] == ag.Token {
		t.Fatalf("reset token: %d %v", c, tr)
	}
	ac.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		if _, _, err := ac.ReadMessage(); err != nil {
			if ce, ok := err.(*websocket.CloseError); !ok || ce.Code != websocket.ClosePolicyViolation {
				t.Fatalf("old connection should be closed with policy violation, got %v", err)
			}
			break
		}
	}
	u := "ws" + strings.TrimPrefix(base, "http") + "/ws?appkey=" + e.tenant.AppKey + "&accid=adm-ag&token="
	if _, resp, err := websocket.DefaultDialer.Dial(u+ag.Token, nil); err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("old token must be rejected: %v %v", err, resp)
	}
	nc, _, err := websocket.DefaultDialer.Dial(u+tr["token"], nil)
	if err != nil {
		t.Fatalf("new token must work: %v", err)
	}
	nc.Close()
	if c := do(t, "POST", base+"/v1/accounts/nobody/token", e.h, nil, nil); c != 404 {
		t.Fatalf("reset unknown: %d", c)
	}

	// ---- 回调投递记录：列表、按状态过滤、仅 dead 可重试 ----
	raw, err := sql.Open("mysql", os.Getenv("TEST_MYSQL_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	res, err := raw.Exec(`INSERT INTO webhook_events(tenant_id,event_type,payload,status,attempts,next_at,last_error) VALUES(?,?,?,?,?,NOW(3),?)`,
		e.tenant.ID, "message.created", "{}", "dead", 8, "status 500")
	if err != nil {
		t.Fatal(err)
	}
	deadID, _ := res.LastInsertId()
	var evs []store.WebhookEventRow
	get("/v1/webhook/events?status=dead", &evs)
	if len(evs) != 1 || evs[0].ID != deadID || evs[0].LastError != "status 500" || evs[0].Attempts != 8 {
		t.Fatalf("dead events: %+v", evs)
	}
	get("/v1/webhook/events?limit=3", &evs)
	if len(evs) == 0 || len(evs) > 3 {
		t.Fatalf("events: %d", len(evs))
	}
	if c := do(t, "POST", fmt.Sprintf("%s/v1/webhook/events/%d/retry", base, deadID), e.h, nil, nil); c != 204 {
		t.Fatalf("retry: %d", c)
	}
	var st string
	raw.QueryRow(`SELECT status FROM webhook_events WHERE id=?`, deadID).Scan(&st)
	if st != "pending" && st != "done" && st != "dead" { // 后台 worker 可能已处理，但不应是别的状态
		t.Fatalf("status after retry: %s", st)
	}
	if c := do(t, "POST", fmt.Sprintf("%s/v1/webhook/events/%d/retry", base, deadID+1000000), e.h, nil, nil); c != 404 {
		t.Fatalf("retry unknown: %d", c)
	}
}
