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

func queueEnv(t *testing.T) *env {
	t.Helper()
	old := store.MinRemindEvery
	store.MinRemindEvery = 1
	t.Cleanup(func() { store.MinRemindEvery = old })
	e := newEnvWith(t, func(s *im.Service) { s.SweepInterval = 100 * time.Millisecond })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go e.svc.Run(ctx) // 后台扫描：排队提醒/告警/放弃都由它驱动
	return e
}

func putQueue(t *testing.T, e *env, body map[string]any) int {
	return do(t, "PUT", e.srv.URL+"/v1/settings/queue", e.h, body, nil)
}

// queueVisitor：访客开始会话并转人工，等到确认进入排队（没有可用坐席）。
func queueVisitor(t *testing.T, e *env) (*websocket.Conn, store.Account, *store.Session) {
	t.Helper()
	v := e.visitor(t)
	c := dial(t, e.srv.URL, e.tenant.AppKey, v)
	t.Cleanup(func() { c.Close() })
	c.WriteJSON(map[string]any{"op": "start"})
	s := wait(t, c, func(x im.Event) bool { return x.Op == "session" }).Session
	c.WriteJSON(map[string]any{"op": "send", "session_id": s.ID, "client_msg_id": "q", "body": "转人工"})
	wait(t, c, func(x im.Event) bool {
		return x.Op == "queue" || (x.Op == "msg" && strings.Contains(x.Message.Body, "转接人工"))
	})
	return c, v, s
}

func countMsgs(t *testing.T, e *env, sid int64, contains string) int {
	msgs, _ := e.st.MessagesSince(context.Background(), e.tenant.ID, sid, 0, 500)
	n := 0
	for _, m := range msgs {
		if m.FromRole == "system" && strings.Contains(m.Body, contains) {
			n++
		}
	}
	return n
}

func TestQueueSettingsAPI(t *testing.T) {
	e := newEnv(t)
	var d store.QueueSettings
	if c := do(t, "GET", e.srv.URL+"/v1/settings/queue", e.h, nil, &d); c != 200 || !d.IsDefault || d.RemindAfter != 60 || d.AlertAfter != 180 || d.GiveupAfter != 0 || d.RemindMax != 3 || !strings.Contains(d.RemindMessage, "{position}") {
		t.Fatalf("defaults: %d %+v", c, d)
	}
	bad := []map[string]any{
		{"remind_after": -1}, {"remind_after": 90000}, {"alert_after": 100000}, {"giveup_after": -5}, {"remind_max": 21}, {"remind_max": -1},
		{"remind_after": 30, "remind_max": 3, "remind_every": 5}, // 重复提醒间隔太短
		{"remind_message": strings.Repeat("字", 201)},
	}
	for _, b := range bad {
		if c := putQueue(t, e, b); c != 400 {
			t.Errorf("%v: want 400, got %d", b, c)
		}
	}
	var saved store.QueueSettings
	if c := do(t, "PUT", e.srv.URL+"/v1/settings/queue", e.h, map[string]any{"remind_after": 30, "remind_every": 20, "remind_max": 2, "alert_after": 90, "giveup_after": 600, "remind_message": "排队中 {position}"}, &saved); c != 200 || saved.IsDefault {
		t.Fatalf("save: %d %+v", c, saved)
	}
	var got store.QueueSettings
	do(t, "GET", e.srv.URL+"/v1/settings/queue", e.h, nil, &got)
	if got.IsDefault || got.RemindAfter != 30 || got.RemindEvery != 20 || got.RemindMax != 2 || got.AlertAfter != 90 || got.GiveupAfter != 600 || got.RemindMessage != "排队中 {position}" || got.GiveupMessage == "" {
		t.Fatalf("persisted: %+v", got) // 空的放弃文案会补上默认值
	}
	// 0 = 关闭
	if c := putQueue(t, e, map[string]any{"remind_after": 0, "alert_after": 0, "giveup_after": 0}); c != 200 {
		t.Fatalf("all off: %d", c)
	}
}

func TestQueueReminderAndAlert(t *testing.T) {
	e := queueEnv(t)
	if c := putQueue(t, e, map[string]any{"remind_after": 1, "remind_every": 1, "remind_max": 2, "alert_after": 2, "giveup_after": 0, "remind_message": "排队第{position}位，前面{ahead}位"}); c != 200 {
		t.Fatalf("settings: %d", c)
	}
	A := mkAgent(t, e, "q-ag", "", 1) // 只能同时接待 1 个
	ac := dial(t, e.srv.URL, e.tenant.AppKey, A)
	defer ac.Close()
	time.Sleep(200 * time.Millisecond)
	cx, _, sx := openHuman(t, e, ac) // 占满坐席
	cy, _, sy := queueVisitor(t, e)
	cz, _, sz := queueVisitor(t, e)
	_, _ = cx, cz

	// 排队顺序提醒：Y 第 1 位、Z 第 2 位，各提醒 2 次（上限）
	wait(t, cy, func(x im.Event) bool { return x.Op == "msg" && x.Message.Body == "排队第1位，前面0位" })
	wait(t, cy, func(x im.Event) bool { return x.Op == "msg" && x.Message.Body == "排队第1位，前面0位" })
	// 坐席收到超时告警（每个排队会话一次）
	alerts := map[int64]int{}
	deadline := time.Now().Add(5 * time.Second)
	for len(alerts) < 2 && time.Now().Before(deadline) {
		ev := wait(t, ac, func(x im.Event) bool { return x.Op == "queue_alert" })
		if ev.Wait < 2 {
			t.Fatalf("alert before threshold: %+v", ev)
		}
		alerts[ev.SessionID]++
	}
	if alerts[sy.ID] == 0 || alerts[sz.ID] == 0 {
		t.Fatalf("both queued sessions should be alerted: %v", alerts)
	}

	// 概览：排队人数、最长等待、超时人数
	var ov struct {
		Queue map[string]int
	}
	do(t, "GET", e.srv.URL+"/v1/stats/overview", e.h, nil, &ov)
	if ov.Queue["count"] != 2 || ov.Queue["overdue"] != 2 || ov.Queue["longest_wait"] < 2 || ov.Queue["alert_after"] != 2 {
		t.Fatalf("overview queue: %v", ov.Queue)
	}
	var sess store.Session
	do(t, "GET", fmt.Sprintf("%s/v1/sessions/%d", e.srv.URL, sy.ID), e.h, nil, &sess)
	if sess.QueuedAt == nil || time.Since(*sess.QueuedAt) < 2*time.Second {
		t.Fatalf("queued_at should mark when queueing started: %+v", sess.QueuedAt)
	}

	// 恰好各 2 次提醒、各 1 次告警回调——不多发
	time.Sleep(2500 * time.Millisecond)
	if n := countMsgs(t, e, sy.ID, "排队第1位"); n != 2 {
		t.Fatalf("Y reminders: %d (want exactly 2 = remind_max)", n)
	}
	if n := countMsgs(t, e, sz.ID, "排队第2位"); n != 2 {
		t.Fatalf("Z reminders: %d (want 2, position 2)", n)
	}
	if evs := e.eventsOf("session.queue_timeout", 2); len(evs) != 2 {
		t.Fatalf("session.queue_timeout events: %d (want 2)", len(evs))
	} else if d := evs[0]["data"].(map[string]any); d["waited_seconds"].(float64) < 2 || d["queue_length"] != float64(2) {
		t.Fatalf("event payload: %v", d)
	}

	// 坐席空出来：队首的 Y 被接待，之后不再收到提醒，QueuedAt 清空
	closeBy(t, ac, cx, sx.ID)
	wait(t, ac, func(x im.Event) bool {
		return x.Op == "session" && x.Session.ID == sy.ID && x.Session.Status == "active"
	})
	before := countMsgs(t, e, sy.ID, "排队第")
	time.Sleep(1500 * time.Millisecond)
	if countMsgs(t, e, sy.ID, "排队第") != before {
		t.Fatal("no more reminders after being assigned")
	}
	var after store.Session // 新变量：JSON 解码到旧结构体时，缺省字段不会被清空
	do(t, "GET", fmt.Sprintf("%s/v1/sessions/%d", e.srv.URL, sy.ID), e.h, nil, &after)
	if after.QueuedAt != nil || after.Status != "active" {
		t.Fatalf("after assignment: %+v", after)
	}
}

func TestQueueGiveup(t *testing.T) {
	e := queueEnv(t)
	putQueue(t, e, map[string]any{"remind_after": 0, "alert_after": 0, "giveup_after": 2, "giveup_message": "没有客服空闲，已转回机器人"})
	c, _, s := queueVisitor(t, e) // 没有任何坐席在线
	ev := wait(t, c, func(x im.Event) bool { return x.Op == "session" && x.Session.ID == s.ID && x.Session.Status == "bot" })
	if ev.Session.QueuedAt != nil {
		t.Fatalf("queued_at must be cleared: %+v", ev.Session)
	}
	if countMsgs(t, e, s.ID, "没有客服空闲") != 1 {
		t.Fatal("giveup message should be sent exactly once")
	}
	if evs := e.eventsOf("session.queue_giveup", 1); len(evs) != 1 {
		t.Fatalf("session.queue_giveup events: %d", len(evs))
	}
	// 退回机器人后能正常对话；再次转人工会重新开始计时，并能再次放弃
	c.WriteJSON(map[string]any{"op": "send", "session_id": s.ID, "client_msg_id": "again", "body": "随便问问"})
	wait(t, c, func(x im.Event) bool {
		return x.Op == "msg" && x.Message.FromRole == "bot" && strings.Contains(x.Message.Body, "抱歉")
	})
	c.WriteJSON(map[string]any{"op": "send", "session_id": s.ID, "client_msg_id": "again2", "body": "转人工"})
	wait(t, c, func(x im.Event) bool { return x.Op == "msg" && strings.Contains(x.Message.Body, "转接人工") })
	var cur store.Session
	do(t, "GET", fmt.Sprintf("%s/v1/sessions/%d", e.srv.URL, s.ID), e.h, nil, &cur)
	if cur.QueuedAt == nil || time.Since(*cur.QueuedAt) > 2*time.Second {
		t.Fatalf("re-queue must restart the clock: %+v", cur.QueuedAt)
	}
	wait(t, c, func(x im.Event) bool { return x.Op == "session" && x.Session.Status == "bot" })
	if countMsgs(t, e, s.ID, "没有客服空闲") != 2 {
		t.Fatal("second giveup message expected")
	}
}

func TestQueueSettingsOff(t *testing.T) {
	e := queueEnv(t)
	putQueue(t, e, map[string]any{"remind_after": 0, "alert_after": 0, "giveup_after": 0})
	_, _, s := queueVisitor(t, e)
	time.Sleep(2500 * time.Millisecond)
	cur, _ := e.st.Session(context.Background(), e.tenant.ID, s.ID)
	if cur.Status != "queued" || countMsgs(t, e, s.ID, "排在第") != 0 {
		t.Fatalf("with everything off nothing should happen: %s", cur.Status)
	}
}

// 坐席把会话退回队列时，也从那一刻开始计排队时间。
func TestQueueClockOnAgentTransfer(t *testing.T) {
	e := queueEnv(t)
	putQueue(t, e, map[string]any{"remind_after": 0, "alert_after": 0, "giveup_after": 0})
	A := mkAgent(t, e, "qt-ag", "", 5)
	ac := dial(t, e.srv.URL, e.tenant.AppKey, A)
	defer ac.Close()
	time.Sleep(200 * time.Millisecond)
	_, _, s := openHuman(t, e, ac)
	// 唯一的坐席把会话退回队列：不会分回给自己，于是留在队列里
	xfer(ac, s.ID, "", "", "")
	wait(t, ac, func(x im.Event) bool { return x.Op == "transfer_ok" })
	var cur store.Session
	do(t, "GET", fmt.Sprintf("%s/v1/sessions/%d", e.srv.URL, s.ID), e.h, nil, &cur)
	if cur.Status != "queued" || cur.QueuedAt == nil || time.Since(*cur.QueuedAt) > 3*time.Second {
		t.Fatalf("transfer-to-queue should start the clock: %+v", cur)
	}
}
