package api_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"npckefu/internal/im"
	"npckefu/internal/store"
)

func mkAgent(t *testing.T, e *env, accid, group string, maxLoad int) store.Account {
	var a store.Account
	do(t, "POST", e.srv.URL+"/v1/accounts", e.h, store.Account{Accid: accid, Name: "客服" + accid, Role: "agent", SkillGroup: group, MaxLoad: maxLoad}, &a)
	return a
}

func xfer(c *websocket.Conn, sid int64, to, group, note string) {
	c.WriteJSON(map[string]any{"op": "transfer", "session_id": sid, "to_agent": to, "skill_group": group, "note": note})
}

func TestAgentTransfer(t *testing.T) {
	e := newEnv(t)
	base := e.srv.URL
	A := mkAgent(t, e, "tA", "alpha", 5)
	B := mkAgent(t, e, "tB", "", 1) // 容量只有 1
	C := mkAgent(t, e, "tC", "vip", 5)
	D := mkAgent(t, e, "tD", "alpha", 5) // 开始时不在线
	ctx := context.Background()
	ownerOf := func(sid int64) string { s, _ := e.st.Session(ctx, e.tenant.ID, sid); return s.AgentAccid }

	ac := dial(t, base, e.tenant.AppKey, A)
	defer ac.Close()
	time.Sleep(200 * time.Millisecond)

	// ---- 1) A → B 直接转接，带备注 ----
	vc1, v1, s1 := openHuman(t, e, ac) // 只有 A 在线 → 分给 A
	bc := dial(t, base, e.tenant.AppKey, B)
	defer bc.Close()
	time.Sleep(200 * time.Millisecond)
	const note = "VIP客户，正在处理退款，请优先跟进"
	xfer(ac, s1.ID, "tB", "", note)
	if ev := wait(t, bc, func(x im.Event) bool { return x.Op == "session" && x.Session.ID == s1.ID }); ev.Session.Status != "active" || ev.Session.AgentAccid != "tB" {
		t.Fatalf("B session: %+v", ev.Session)
	}
	if ev := wait(t, bc, func(x im.Event) bool { return x.Op == "transferred" }); ev.From != "tA" || ev.Note != note || ev.SessionID != s1.ID {
		t.Fatalf("B transferred: %+v", ev)
	}
	// 原坐席收到"会话已归他人"的通知；访客看到转接提示
	if ev := wait(t, ac, func(x im.Event) bool {
		return x.Op == "session" && x.Session.ID == s1.ID && x.Session.AgentAccid == "tB"
	}); ev.Session.Status != "active" {
		t.Fatalf("A notified: %+v", ev.Session)
	}
	wait(t, ac, func(x im.Event) bool { return x.Op == "transfer_ok" && x.SessionID == s1.ID })
	wait(t, vc1, func(x im.Event) bool {
		return x.Op == "msg" && x.Message.FromRole == "system" && strings.Contains(x.Message.Body, "转接给客服")
	})
	// 原坐席不能再发言，新坐席可以
	ac.WriteJSON(map[string]any{"op": "send", "session_id": s1.ID, "client_msg_id": "x", "body": "我还在吗"})
	wantErr(t, ac, "forbidden")
	bc.WriteJSON(map[string]any{"op": "send", "session_id": s1.ID, "client_msg_id": "b1", "body": "我是新的客服"})
	wait(t, vc1, func(x im.Event) bool { return x.Op == "msg" && x.Message.From == "tB" })
	if ownerOf(s1.ID) != "tB" {
		t.Fatalf("owner: %s", ownerOf(s1.ID))
	}
	// 备注绝不能泄露给访客：消息记录里没有
	msgs, _ := e.st.MessagesSince(ctx, e.tenant.ID, s1.ID, 0, 100)
	for _, m := range msgs {
		if strings.Contains(m.Body, "退款") {
			t.Fatalf("note leaked into messages: %q", m.Body)
		}
	}
	_ = v1

	// ---- 2) 失败场景（B 已满载 1/1；v2 分给 A）----
	vc2, v2, s2 := openHuman(t, e, ac)
	cases := []struct {
		name, to, group, want string
	}{
		{"目标已满载", "tB", "", "不在线或已满载"},
		{"目标离线", "tD", "", "不在线或已满载"},
		{"转给自己", "tA", "", "bad input"},
		{"目标不存在", "nobody", "", "不存在"},
		{"目标是访客", v2.Accid, "", "不存在"},
		{"技能组不存在", "", "nope", "不存在"},
		{"同时指定坐席和技能组", "tC", "vip", "bad input"},
	}
	for _, c := range cases {
		xfer(ac, s2.ID, c.to, c.group, "")
		if er := wait(t, ac, func(x im.Event) bool { return x.Op == "error" }); !strings.Contains(er.Error, c.want) {
			t.Errorf("%s: error %q, want contains %q", c.name, er.Error, c.want)
		}
	}
	if ownerOf(s2.ID) != "tA" {
		t.Fatalf("failed transfers must not change owner: %s", ownerOf(s2.ID))
	}
	xfer(bc, s2.ID, "tC", "", "") // 非当前接待坐席
	wantErr(t, bc, "forbidden")
	vc2.WriteJSON(map[string]any{"op": "transfer", "session_id": s2.ID, "to_agent": "tC"}) // 访客不能转接
	wantErr(t, vc2, "forbidden")
	longNote := strings.Repeat("备", 501)
	xfer(ac, s2.ID, "tC", "", longNote)
	wantErr(t, ac, "bad input")

	// ---- 3) 退回队列：不会分回原坐席 ----
	cc := dial(t, base, e.tenant.AppKey, C)
	defer cc.Close()
	time.Sleep(200 * time.Millisecond)
	xfer(ac, s2.ID, "", "", "客户情绪激动，请安抚")
	wait(t, ac, func(x im.Event) bool { return x.Op == "transfer_ok" })
	if ev := wait(t, cc, func(x im.Event) bool { return x.Op == "session" && x.Session.ID == s2.ID }); ev.Session.AgentAccid != "tC" {
		t.Fatalf("should go to C, not back to A (B is full): %+v", ev.Session)
	}
	if ev := wait(t, cc, func(x im.Event) bool { return x.Op == "transferred" }); ev.From != "tA" || ev.Note != "客户情绪激动，请安抚" {
		t.Fatalf("queue-transfer note must reach the new agent: %+v", ev)
	}
	wait(t, vc2, func(x im.Event) bool { return x.Op == "msg" && strings.Contains(x.Message.Body, "转接其他客服") })

	// ---- 4) 转到技能组：组内只有原坐席在线 → 排队等待；D 上线后接手 ----
	_, _, s3 := openHuman(t, e, ac) // A(0) 与 C(1)：分给 A
	xfer(ac, s3.ID, "", "alpha", "需要 alpha 组处理")
	wait(t, ac, func(x im.Event) bool { return x.Op == "transfer_ok" })
	time.Sleep(600 * time.Millisecond) // 含兜底扫描：A 被排除，D 离线
	if s, _ := e.st.Session(ctx, e.tenant.ID, s3.ID); s.Status != "queued" || s.SkillGroup != "alpha" {
		t.Fatalf("should wait in queue for another alpha agent: %+v", s)
	}
	dc := dial(t, base, e.tenant.AppKey, D)
	defer dc.Close()
	if ev := wait(t, dc, func(x im.Event) bool { return x.Op == "session" && x.Session.ID == s3.ID }); ev.Session.AgentAccid != "tD" {
		t.Fatalf("D should take it: %+v", ev.Session)
	}
	if ev := wait(t, dc, func(x im.Event) bool { return x.Op == "transferred" }); ev.From != "tA" || ev.Note != "需要 alpha 组处理" {
		t.Fatalf("D note: %+v", ev)
	}

	// ---- 5) 租户/主管代操作 + 审计 + 事件 ----
	var moved store.Session
	if c := do(t, "POST", fmt.Sprintf("%s/v1/sessions/%d/transfer", base, s1.ID), e.h, map[string]any{"to_agent": "tC", "note": "主管调度"}, &moved); c != 200 || moved.AgentAccid != "tC" {
		t.Fatalf("on-behalf transfer: %d %+v", c, moved)
	}
	var hist []store.Transfer
	do(t, "GET", fmt.Sprintf("%s/v1/sessions/%d/transfers", base, s1.ID), e.h, nil, &hist)
	if len(hist) != 2 || hist[0].FromAgent != "tA" || hist[0].ToAgent != "tB" || hist[0].Note != note || hist[1].FromAgent != "tB" || hist[1].ToAgent != "tC" {
		t.Fatalf("history: %+v", hist)
	}
	if evs := e.eventsOf("session.transferred", 4); len(evs) < 4 { // s1×2, s2, s3
		t.Fatalf("session.transferred events: %d", len(evs))
	} else {
		d := evs[0]["data"].(map[string]any)
		if d["from_agent"] != "tA" || d["to_agent"] != "tB" || d["note"] != note {
			t.Fatalf("event payload: %v", d)
		}
	}
	for _, c := range []struct {
		body map[string]any
		want int
	}{
		{map[string]any{"to_agent": "nobody"}, 422},
		{map[string]any{"to_agent": "tC", "skill_group": "vip"}, 400},
		{map[string]any{"to_agent": "tC"}, 400}, // 已经是当前接待坐席
	} {
		if code := do(t, "POST", fmt.Sprintf("%s/v1/sessions/%d/transfer", base, s1.ID), e.h, c.body, nil); code != c.want {
			t.Errorf("%v: got %d want %d", c.body, code, c.want)
		}
	}
	if c := do(t, "POST", base+"/v1/sessions/99999999/transfer", e.h, map[string]any{}, nil); c != 404 {
		t.Fatalf("unknown session: %d", c)
	}

	// ---- 6) 转接次数上限（防止来回踢皮球）----
	for i := 0; i < 8; i++ { // 已有 2 次，再转 8 次到 10
		to := "tD"
		if i%2 == 1 {
			to = "tC"
		}
		if c := do(t, "POST", fmt.Sprintf("%s/v1/sessions/%d/transfer", base, s1.ID), e.h, map[string]any{"to_agent": to}, nil); c != 200 {
			t.Fatalf("transfer #%d: %d", i+3, c)
		}
	}
	if c := do(t, "POST", fmt.Sprintf("%s/v1/sessions/%d/transfer", base, s1.ID), e.h, map[string]any{"to_agent": "tA"}, nil); c != 409 {
		t.Fatalf("11th transfer should be 409, got %d", c)
	}

	// ---- 7) 已结束的会话不能转接 ----
	closeBy(t, vc1, vc1, s1.ID)
	if c := do(t, "POST", fmt.Sprintf("%s/v1/sessions/%d/transfer", base, s1.ID), e.h, map[string]any{"to_agent": "tA"}, nil); c != 409 {
		t.Fatalf("closed session: %d", c)
	}
}

// 名单：在线状态、负载、技能组；非坐席无权查看。
func TestAgentRoster(t *testing.T) {
	e := newEnv(t)
	A := mkAgent(t, e, "rA", "alpha", 3)
	mkAgent(t, e, "rB", "beta", 2)
	ac := dial(t, e.srv.URL, e.tenant.AppKey, A)
	defer ac.Close()
	time.Sleep(200 * time.Millisecond)
	ac.WriteJSON(map[string]any{"op": "agents"})
	ev := wait(t, ac, func(x im.Event) bool { return x.Op == "agents" })
	if len(ev.Agents) != 2 || ev.Agents[0].Accid != "rA" || !ev.Agents[0].Online || ev.Agents[0].MaxLoad != 3 || ev.Agents[0].SkillGroup != "alpha" || ev.Agents[1].Online {
		t.Fatalf("roster: %+v", ev.Agents)
	}
	vc := dial(t, e.srv.URL, e.tenant.AppKey, e.visitor(t))
	defer vc.Close()
	vc.WriteJSON(map[string]any{"op": "agents"})
	wantErr(t, vc, "forbidden")
}

// 并发转接不能把容量只有 1 的坐席转超：容量检查与改派在同一把分配锁内。
func TestTransferCapacityRace(t *testing.T) {
	e := newEnv(t)
	A := mkAgent(t, e, "cA", "", 10)
	B := mkAgent(t, e, "cB", "", 1)
	ac := dial(t, e.srv.URL, e.tenant.AppKey, A)
	defer ac.Close()
	time.Sleep(200 * time.Millisecond)
	var sids []int64
	for i := 0; i < 6; i++ { // 只有 A 在线：6 个会话都归 A
		_, _, s := openHuman(t, e, ac)
		sids = append(sids, s.ID)
	}
	bc := dial(t, e.srv.URL, e.tenant.AppKey, B)
	defer bc.Close()
	time.Sleep(200 * time.Millisecond)

	codes := make(chan int, len(sids))
	for _, sid := range sids {
		go func(sid int64) {
			codes <- do(t, "POST", fmt.Sprintf("%s/v1/sessions/%d/transfer", e.srv.URL, sid), e.h, map[string]any{"to_agent": "cB"}, nil)
		}(sid)
	}
	ok := 0
	for range sids {
		switch c := <-codes; c {
		case 200:
			ok++
		case 409:
		default:
			t.Fatalf("unexpected status %d", c)
		}
	}
	load, _ := e.st.ActiveLoad(context.Background(), e.tenant.ID)
	if ok != 1 || load["cB"] != 1 || load["cA"] != 5 {
		t.Fatalf("ok=%d load=%v (B has capacity 1)", ok, load)
	}
}
