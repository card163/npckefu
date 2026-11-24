package api_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"npckefu/internal/im"
	"npckefu/internal/store"
)

type wlReport struct {
	Agents []struct {
		store.AgentWorkload
		Name   string `json:"name"`
		Online bool   `json:"online"`
	} `json:"agents"`
	Totals struct {
		Sessions         int      `json:"sessions"`
		Assignments      int      `json:"assignments"`
		Replied          int      `json:"replied"`
		AvgFirstReply    *float64 `json:"avg_first_reply_seconds"`
		AvgHandle        *float64 `json:"avg_handle_seconds"`
		Messages         int      `json:"messages"`
		Transfers        int      `json:"transfers"`
		Ratings          int      `json:"ratings"`
		AvgRating        *float64 `json:"avg_rating"`
		SatisfactionRate *float64 `json:"satisfaction_rate"`
	} `json:"totals"`
	Daily []store.DayStat `json:"daily"`
}

func TestAgentWorkloadReport(t *testing.T) {
	e := newEnv(t)
	base := e.srv.URL
	ctx := context.Background()
	// A 的名字以 = 开头：CSV 里必须被中和
	var A, B store.Account
	do(t, "POST", base+"/v1/accounts", e.h, store.Account{Accid: "wl-a", Name: "=SUM(1)", Role: "agent", MaxLoad: 10}, &A)
	do(t, "POST", base+"/v1/accounts", e.h, store.Account{Accid: "wl-b", Name: "乙", Role: "agent", MaxLoad: 10}, &B)
	ac := dial(t, base, e.tenant.AppKey, A)
	defer ac.Close()
	time.Sleep(200 * time.Millisecond)

	// 只有 A 在线：5 个会话全部分给 A（dispatch）
	var cs [6]*websocket.Conn
	var ss [6]*store.Session
	for i := 1; i <= 5; i++ {
		c, _, s := openHuman(t, e, ac)
		defer c.Close()
		cs[i], ss[i] = c, s
	}
	reply := func(c *websocket.Conn, sid int64, id, body string) {
		c.WriteJSON(map[string]any{"op": "send", "session_id": sid, "client_msg_id": id, "body": body})
	}
	reply(ac, ss[1].ID, "r1", "您好")
	reply(ac, ss[2].ID, "r2", "在的")
	reply(ac, ss[3].ID, "r3a", "稍等")
	reply(ac, ss[3].ID, "r3b", "已处理")
	for _, i := range []int{1, 2, 3} {
		wait(t, cs[i], func(x im.Event) bool { return x.Op == "msg" && x.Message.From == "wl-a" })
	}
	time.Sleep(300 * time.Millisecond)

	bc := dial(t, base, e.tenant.AppKey, B)
	defer bc.Close()
	time.Sleep(200 * time.Millisecond)
	// s2：A 直接转给 B；s5：A 退回队列，B 接手（A 被排除）
	xfer(ac, ss[2].ID, "wl-b", "", "")
	wait(t, ac, func(x im.Event) bool { return x.Op == "transfer_ok" && x.SessionID == ss[2].ID })
	xfer(ac, ss[5].ID, "", "", "")
	wait(t, ac, func(x im.Event) bool { return x.Op == "transfer_ok" && x.SessionID == ss[5].ID })
	wait(t, bc, func(x im.Event) bool {
		return x.Op == "session" && x.Session.ID == ss[5].ID && x.Session.Status == "active"
	})
	reply(bc, ss[2].ID, "b1", "我是乙")
	wait(t, cs[2], func(x im.Event) bool { return x.Op == "msg" && x.Message.From == "wl-b" })

	// 结束：s1 由 A 结束并评 5 分；s2 由 B 结束并评 3 分；s3、s4、s5 由租户结束
	closeBy(t, ac, cs[1], ss[1].ID)
	rateWS(cs[1], ss[1].ID, 5, "")
	wait(t, cs[1], func(x im.Event) bool { return x.Op == "rated" })
	closeBy(t, bc, cs[2], ss[2].ID)
	rateWS(cs[2], ss[2].ID, 3, "")
	wait(t, cs[2], func(x im.Event) bool { return x.Op == "rated" })
	for _, i := range []int{3, 4, 5} {
		if c := do(t, "POST", fmt.Sprintf("%s/v1/sessions/%d/close", base, ss[i].ID), e.h, nil, nil); c != 204 {
			t.Fatalf("close %d: %d", i, c)
		}
	}

	get := func(q url.Values) (wlReport, int) {
		var r wlReport
		c := do(t, "GET", base+"/v1/stats/agents?"+q.Encode(), e.h, nil, &r)
		return r, c
	}
	f := func(p *float64) float64 {
		if p == nil {
			t.Fatal("expected a number, got null")
		}
		return *p
	}

	rep, c := get(url.Values{"tz": {"UTC"}})
	if c != 200 || len(rep.Agents) != 2 || rep.Agents[0].AgentAccid != "wl-a" || rep.Agents[1].AgentAccid != "wl-b" {
		t.Fatalf("agents (sorted by sessions desc): %d %+v", c, rep.Agents)
	}
	a, b := rep.Agents[0], rep.Agents[1]
	if a.Sessions != 5 || a.Assignments != 5 || a.Replied != 3 || a.Ended != 5 || a.TransferredOut != 2 || a.TransferredIn != 0 || a.Messages != 4 || a.Ratings != 1 || f(a.AvgRating) != 5 || f(a.SatisfactionRate) != 1 || !a.Online || a.Name != "=SUM(1)" {
		t.Fatalf("A: %+v", a)
	}
	if b.Sessions != 2 || b.Assignments != 2 || b.Replied != 1 || b.Ended != 2 || b.TransferredIn != 2 || b.TransferredOut != 0 || b.Messages != 1 || b.Ratings != 1 || f(b.AvgRating) != 3 || f(b.SatisfactionRate) != 0 {
		t.Fatalf("B: %+v", b) // 转入 2 = 直接转接 s2 + 退回队列后接手的 s5
	}
	if f(a.AvgFirstReply) < 0 || f(a.AvgFirstReply) > 30 || f(a.MaxFirstReply) < f(a.AvgFirstReply) || f(a.AvgHandle) < 0 {
		t.Fatalf("A timings: %+v", a)
	}
	tot := rep.Totals
	if tot.Sessions != 5 || tot.Assignments != 7 || tot.Replied != 4 || tot.Messages != 5 || tot.Transfers != 2 || tot.Ratings != 2 || f(tot.AvgRating) != 4 || f(tot.SatisfactionRate) != 0.5 || tot.AvgFirstReply == nil || tot.AvgHandle == nil {
		t.Fatalf("totals: %+v", tot) // 会话数去重：s2 被两个坐席接待，只算 1 个
	}

	// 每天：默认最近 7 天 = 连续 8 个日期（含今天），只有今天有数据
	today := time.Now().UTC().Format("2006-01-02")
	if len(rep.Daily) != 8 || rep.Daily[7].Date != today || rep.Daily[7].Sessions != 5 || rep.Daily[7].Messages != 5 {
		t.Fatalf("daily: %+v", rep.Daily)
	}
	for _, d := range rep.Daily[:7] {
		if d.Sessions != 0 || d.Messages != 0 {
			t.Fatalf("earlier days must be zero-filled: %+v", d)
		}
	}
	// 时区：横轴日期按指定时区
	sh, _ := time.LoadLocation("Asia/Shanghai")
	repSH, _ := get(url.Values{"tz": {"Asia/Shanghai"}})
	last := repSH.Daily[len(repSH.Daily)-1]
	if last.Date != time.Now().In(sh).Format("2006-01-02") || last.Sessions != 5 {
		t.Fatalf("Shanghai daily: %+v", last)
	}

	// 指定坐席
	repB, _ := get(url.Values{"tz": {"UTC"}, "agent": {"wl-b"}})
	if len(repB.Agents) != 1 || repB.Totals.Sessions != 2 || repB.Totals.Messages != 1 || repB.Daily[len(repB.Daily)-1].Sessions != 2 || repB.Daily[len(repB.Daily)-1].Messages != 1 {
		t.Fatalf("agent filter: %+v", repB)
	}
	// 没有活动的区间：名单里的坐席仍列出，数值为 0，平均值为 null
	empty, _ := get(url.Values{"from": {"2999-01-01"}, "to": {"2999-01-03"}, "tz": {"UTC"}})
	if len(empty.Agents) != 2 || empty.Agents[0].Sessions != 0 || empty.Agents[0].AvgFirstReply != nil || empty.Totals.Sessions != 0 || empty.Totals.AvgRating != nil || len(empty.Daily) != 3 {
		t.Fatalf("empty range: %+v", empty)
	}

	// 参数校验
	for _, bad := range []url.Values{{"tz": {"Mars/Base"}}, {"format": {"xlsx"}}, {"from": {"2030-01-02"}, "to": {"2030-01-01"}}, {"from": {"2020-01-01"}, "to": {"2022-01-01"}}, {"from": {"yesterday"}}} {
		if _, c := get(bad); c != 400 {
			t.Errorf("%v: want 400, got %d", bad, c)
		}
	}

	// CSV
	st, hdr, body := getRaw(t, base+"/v1/stats/agents?format=csv&tz=UTC", e.h)
	if st != 200 || !strings.HasPrefix(hdr.Get("Content-Type"), "text/csv") || !strings.Contains(hdr.Get("Content-Disposition"), "agent-workload-") {
		t.Fatalf("csv: %d %v", st, hdr)
	}
	rows := parseCSV(t, body)
	head := rows[0]
	if len(rows) != 3 || rows[1][col(head, "agent_accid")] != "wl-a" || rows[1][col(head, "name")] != "'=SUM(1)" || rows[1][col(head, "sessions")] != "5" || rows[2][col(head, "transferred_in")] != "2" || rows[2][col(head, "satisfaction_rate")] != "0" {
		t.Fatalf("csv rows: %v", rows)
	}

	// ---- 底层记录：首次回复只记第一条；转接来源 ----
	raw, err := sql.Open("mysql", os.Getenv("TEST_MYSQL_DSN")+"?parseTime=true&loc=UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var firstReply time.Time
	if err := raw.QueryRow(`SELECT first_reply_at FROM session_assignments WHERE session_id=? AND agent_accid='wl-a'`, ss[3].ID).Scan(&firstReply); err != nil {
		t.Fatal(err)
	}
	msgs, _ := e.st.MessagesSince(ctx, e.tenant.ID, ss[3].ID, 0, 100)
	var firstAgentMsg time.Time
	for _, m := range msgs {
		if m.FromRole == "agent" {
			firstAgentMsg = m.CreatedAt
			break
		}
	}
	if d := firstReply.Sub(firstAgentMsg); d < -time.Millisecond || d > time.Millisecond {
		t.Fatalf("first_reply_at must be the FIRST agent message: %v vs %v", firstReply, firstAgentMsg)
	}
	var nTransfer, nDispatch, nOpen int
	raw.QueryRow(`SELECT SUM(source='transfer'), SUM(source='dispatch'), SUM(ended_at IS NULL) FROM session_assignments WHERE tenant_id=?`, e.tenant.ID).Scan(&nTransfer, &nDispatch, &nOpen)
	if nTransfer != 2 || nDispatch != 5 || nOpen != 0 {
		t.Fatalf("sources: transfer=%d dispatch=%d open=%d", nTransfer, nDispatch, nOpen)
	}
}
