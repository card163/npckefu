package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"npckefu/internal/api"
	"npckefu/internal/bot"
	"npckefu/internal/im"
	"npckefu/internal/store"
	"npckefu/internal/webhook"
)

type env struct {
	srv    *httptest.Server
	st     *store.Store
	svc    *im.Service
	tenant store.Tenant
	h      map[string]string
	mu     sync.Mutex
	events []map[string]any // 回调收到的事件 envelope
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN")
	}
	st, err := store.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	e := &env{st: st}
	e.svc = im.NewService(st, bot.New(st, nil), nil)
	e.svc.PushInterval = time.Hour
	e.svc.PresenceGrace = 300 * time.Millisecond
	a := &api.API{St: st, IM: e.svc, AdminToken: "adm", AllowPrivateWebhook: true}
	e.srv = httptest.NewServer(a.Handler())
	t.Cleanup(e.srv.Close)
	recv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		json.Unmarshal(b, &m)
		e.mu.Lock()
		e.events = append(e.events, m)
		e.mu.Unlock()
	}))
	t.Cleanup(recv.Close)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	wk := webhook.New(st, true)
	wk.Poll = 50 * time.Millisecond
	go wk.Run(ctx)

	do(t, "POST", e.srv.URL+"/v1/tenants", map[string]string{"X-Admin-Token": "adm"}, map[string]string{"name": "safety"}, &e.tenant)
	e.h = map[string]string{"X-App-Key": e.tenant.AppKey, "X-App-Secret": e.tenant.AppSecret}
	do(t, "PUT", e.srv.URL+"/v1/webhook", e.h, map[string]string{"url": recv.URL}, nil)
	return e
}

func (e *env) visitor(t *testing.T) store.Account {
	var v store.Account
	do(t, "POST", e.srv.URL+"/v1/visitors/register", nil, map[string]string{"app_key": e.tenant.AppKey}, &v)
	return v
}

// eventsOf 等待片刻后返回某类型事件。
func (e *env) eventsOf(typ string, wantAtLeast int) []map[string]any {
	deadline := time.Now().Add(3 * time.Second)
	for {
		e.mu.Lock()
		var out []map[string]any
		for _, ev := range e.events {
			if ev["type"] == typ {
				out = append(out, ev)
			}
		}
		e.mu.Unlock()
		if len(out) >= wantAtLeast || time.Now().After(deadline) {
			return out
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestModeration(t *testing.T) {
	e := newEnv(t)
	u := e.srv.URL + "/v1/sensitive-words"

	if c := do(t, "POST", u, e.h, map[string]any{"words": []string{"a"}}, nil); c != 400 {
		t.Fatalf("single-char word should be 400, got %d", c)
	}
	if c := do(t, "POST", u, e.h, map[string]any{"words": []string{"傻逼"}, "action": "ban"}, nil); c != 400 {
		t.Fatalf("bad action should be 400, got %d", c)
	}
	do(t, "POST", u, e.h, map[string]any{"words": []string{"傻逼", "骂人话"}}, nil)
	do(t, "POST", u, e.h, map[string]any{"words": []string{"赌博网站"}, "action": "block"}, nil)
	var words []store.SensitiveWord
	do(t, "GET", u, e.h, nil, &words)
	if len(words) != 3 {
		t.Fatalf("words: %+v", words)
	}

	v := e.visitor(t)
	vc := dial(t, e.srv.URL, e.tenant.AppKey, v)
	defer vc.Close()
	vc.WriteJSON(map[string]any{"op": "start"})
	sess := wait(t, vc, func(x im.Event) bool { return x.Op == "session" }).Session

	// 屏蔽：夹空格的变体也命中，落库与广播的都是屏蔽后的文本
	vc.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "m1", "body": "你是傻 逼吗"})
	ack := wait(t, vc, func(x im.Event) bool { return x.Op == "ack" && x.ClientMsgID == "m1" })
	if ack.Message.Body != "你是***吗" {
		t.Fatalf("masked body: %q", ack.Message.Body)
	}
	stored, _ := e.st.MessagesSince(context.Background(), e.tenant.ID, sess.ID, 0, 100)
	for _, m := range stored {
		if strings.Contains(m.Body, "傻") {
			t.Fatalf("original text leaked into storage: %q", m.Body)
		}
	}

	// 拒绝：不落库，客户端收到 error
	before := len(stored)
	vc.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "m2", "body": "来赌博网站吧"})
	er := wait(t, vc, func(x im.Event) bool { return x.Op == "error" && x.ClientMsgID == "m2" })
	if !strings.Contains(er.Error, "违规") {
		t.Fatalf("error: %q", er.Error)
	}
	after, _ := e.st.MessagesSince(context.Background(), e.tenant.ID, sess.ID, 0, 100)
	for _, m := range after {
		if strings.Contains(m.Body, "赌博") {
			t.Fatal("blocked message was stored")
		}
	}
	_ = before

	// 审计事件
	evs := e.eventsOf("message.moderated", 2)
	if len(evs) < 2 {
		t.Fatalf("moderated events: %d", len(evs))
	}
	actions := map[string]bool{}
	for _, ev := range evs {
		d := ev["data"].(map[string]any)
		actions[d["action"].(string)] = true
	}
	if !actions["masked"] || !actions["blocked"] {
		t.Fatalf("actions: %v", actions)
	}

	// 删除词后立即生效（本实例缓存失效）
	for _, w := range words {
		if w.Word == "傻逼" {
			do(t, "DELETE", fmt.Sprintf("%s/%d", u, w.ID), e.h, nil, nil)
		}
	}
	vc.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "m3", "body": "傻逼"})
	ack = wait(t, vc, func(x im.Event) bool { return x.Op == "ack" && x.ClientMsgID == "m3" })
	if ack.Message.Body != "傻逼" {
		t.Fatalf("after delete body: %q", ack.Message.Body)
	}
}

func TestOfflinePush(t *testing.T) {
	e := newEnv(t)
	off, on := e.visitor(t), e.visitor(t)
	dev := func(acc string) string { return e.srv.URL + "/v1/accounts/" + acc + "/devices" }

	if c := do(t, "PUT", dev(off.Accid), e.h, map[string]string{"platform": "", "token": "x"}, nil); c != 400 {
		t.Fatalf("empty platform should be 400, got %d", c)
	}
	if c := do(t, "PUT", dev("nobody"), e.h, map[string]string{"platform": "ios", "token": "x"}, nil); c != 404 {
		t.Fatalf("unknown account should be 404, got %d", c)
	}
	do(t, "PUT", dev(off.Accid), e.h, map[string]string{"platform": "ios", "token": "tok-ios"}, nil)
	do(t, "PUT", dev(off.Accid), e.h, map[string]string{"platform": "huawei", "token": "tok-hw"}, nil)
	do(t, "PUT", dev(on.Accid), e.h, map[string]string{"platform": "android", "token": "tok-on"}, nil)

	// on 访客保持在线
	oc := dial(t, e.srv.URL, e.tenant.AppKey, on)
	defer oc.Close()

	notice := func(to, body string) {
		if c := do(t, "POST", e.srv.URL+"/v1/messages", e.h, map[string]string{"to": to, "body": body}, nil); c != 200 {
			t.Fatalf("notice: %d", c)
		}
	}
	notice(off.Accid, "您的订单已发货")
	notice(off.Accid, "快递已到达")   // 合并推送：同会话同账号间隔内不重复
	notice(on.Accid, "您在线，不应推送") // 在线用户不推

	evs := e.eventsOf("push.offline", 1)
	time.Sleep(400 * time.Millisecond) // 留出时间让不该出现的事件出现
	evs = e.eventsOf("push.offline", 1)
	if len(evs) != 1 {
		t.Fatalf("want exactly 1 push.offline, got %d", len(evs))
	}
	d := evs[0]["data"].(map[string]any)
	if d["accid"] != off.Accid || d["body"] != "您的订单已发货" || len(d["devices"].([]any)) != 2 {
		t.Fatalf("push payload: %+v", d)
	}

	// token 换号：同一 token 登记给另一账号后，原账号不再收到
	do(t, "PUT", dev(on.Accid), e.h, map[string]string{"platform": "ios", "token": "tok-ios"}, nil)
	devs, _ := e.st.DevicesOf(context.Background(), e.tenant.ID, off.Accid)
	if len(devs) != 1 || devs[0].Token != "tok-hw" {
		t.Fatalf("after transfer: %+v", devs)
	}
	// 删除
	do(t, "DELETE", dev(off.Accid)+"/tok-hw", e.h, nil, nil)
	if devs, _ := e.st.DevicesOf(context.Background(), e.tenant.ID, off.Accid); len(devs) != 0 {
		t.Fatalf("after delete: %+v", devs)
	}
	// 每账号最多 10 台，保留最近的
	for i := 0; i < 12; i++ {
		do(t, "PUT", dev(off.Accid), e.h, map[string]string{"platform": "ios", "token": fmt.Sprintf("bulk-%02d", i)}, nil)
		time.Sleep(5 * time.Millisecond)
	}
	if devs, _ := e.st.DevicesOf(context.Background(), e.tenant.ID, off.Accid); len(devs) != 10 {
		t.Fatalf("device cap: %d", len(devs))
	}

	// 客户端自己经 WS 登记
	oc.WriteJSON(map[string]any{"op": "device", "platform": "android", "token": "ws-token"})
	time.Sleep(300 * time.Millisecond)
	found := false
	devs, _ = e.st.DevicesOf(context.Background(), e.tenant.ID, on.Accid)
	for _, x := range devs {
		found = found || x.Token == "ws-token"
	}
	if !found {
		t.Fatal("ws device registration failed")
	}
}

func TestPresence(t *testing.T) {
	e := newEnv(t)
	v := e.visitor(t)
	var agent store.Account
	do(t, "POST", e.srv.URL+"/v1/accounts", e.h, store.Account{Accid: "ag-presence", Name: "坐席", Role: "agent"}, &agent)
	get := func(accid string) (im.Presence, int) {
		var p im.Presence
		c := do(t, "GET", e.srv.URL+"/v1/accounts/"+accid+"/presence", e.h, nil, &p)
		return p, c
	}

	// 从未上线
	p, c := get(v.Accid)
	if c != 200 || !p.Exists || p.Online || p.Connections != 0 || p.LastSeenAt != nil || p.Role != "visitor" {
		t.Fatalf("initial: %d %+v", c, p)
	}
	if _, c := get("nobody"); c != 404 {
		t.Fatalf("unknown should be 404, got %d", c)
	}

	// 两端同时在线
	c1 := dial(t, e.srv.URL, e.tenant.AppKey, v)
	c2 := dial(t, e.srv.URL, e.tenant.AppKey, v)
	time.Sleep(200 * time.Millisecond)
	if p, _ = get(v.Accid); !p.Online || p.Connections != 2 || p.LastSeenAt != nil {
		t.Fatalf("two devices: %+v", p)
	}
	// 坐席列表带在线状态
	ac := dial(t, e.srv.URL, e.tenant.AppKey, agent)
	time.Sleep(200 * time.Millisecond)
	var list []map[string]any
	do(t, "GET", e.srv.URL+"/v1/accounts?role=agent", e.h, nil, &list)
	if len(list) == 0 || list[0]["online"] != true {
		t.Fatalf("agent list: %+v", list)
	}

	// 批量：按输入顺序，未知账号 exists=false；数量校验
	var batch []im.Presence
	do(t, "POST", e.srv.URL+"/v1/presence", e.h, map[string]any{"accids": []string{"nobody", v.Accid, agent.Accid}}, &batch)
	if len(batch) != 3 || batch[0].Exists || !batch[1].Online || !batch[2].Online || batch[2].Role != "agent" {
		t.Fatalf("batch: %+v", batch)
	}
	if c := do(t, "POST", e.srv.URL+"/v1/presence", e.h, map[string]any{"accids": []string{}}, nil); c != 400 {
		t.Fatalf("empty batch should be 400, got %d", c)
	}

	// 关掉一端仍在线；全部关掉后离线并有 last_seen
	c1.Close()
	time.Sleep(200 * time.Millisecond)
	if p, _ = get(v.Accid); !p.Online || p.Connections != 1 {
		t.Fatalf("one left: %+v", p)
	}
	c2.Close()
	time.Sleep(200 * time.Millisecond)
	if p, _ = get(v.Accid); p.Online || p.Connections != 0 || p.LastSeenAt == nil || time.Since(*p.LastSeenAt) > time.Minute {
		t.Fatalf("offline: %+v", p)
	}
	ac.Close()

	// 事件：visitor 上线 1 次、离线 1 次（宽限期后）
	count := func(accid string, online bool) int {
		n := 0
		for _, ev := range e.eventsOf("presence.changed", 1) {
			d := ev["data"].(map[string]any)
			if d["accid"] == accid && d["online"] == online {
				n++
			}
		}
		return n
	}
	time.Sleep(600 * time.Millisecond) // 超过 PresenceGrace
	if count(v.Accid, true) != 1 || count(v.Accid, false) != 1 {
		t.Fatalf("events: online=%d offline=%d", count(v.Accid, true), count(v.Accid, false))
	}

	// 抖动：断线后在宽限期内重连，不应产生离线/上线事件
	f := e.visitor(t)
	fc := dial(t, e.srv.URL, e.tenant.AppKey, f)
	time.Sleep(150 * time.Millisecond)
	fc.Close()
	time.Sleep(100 * time.Millisecond)
	fc = dial(t, e.srv.URL, e.tenant.AppKey, f) // 宽限期内重连
	defer fc.Close()
	time.Sleep(700 * time.Millisecond)
	if count(f.Accid, true) != 1 || count(f.Accid, false) != 0 {
		t.Fatalf("flap events: online=%d offline=%d", count(f.Accid, true), count(f.Accid, false))
	}
	if p, _ = get(f.Accid); !p.Online {
		t.Fatalf("after reconnect: %+v", p)
	}
}
