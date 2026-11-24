package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"npckefu/internal/api"
	"npckefu/internal/bot"
	"npckefu/internal/im"
	"npckefu/internal/store"
	"npckefu/internal/webhook"
)

type hook struct {
	Type string
	ID   string
	Sig  string
	TS   string
	Body []byte
}

func do(t *testing.T, method, url string, h map[string]string, body any, out any) int {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytesReader(b)
	}
	req, _ := http.NewRequest(method, url, rd)
	for k, v := range h {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil {
		json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}

func TestWebhookNoticeRecent(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN")
	}
	st, err := store.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc := im.NewService(st, bot.New(st, nil))
	a := &api.API{St: st, IM: svc, AdminToken: "adm", AllowPrivateWebhook: true}
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()

	// 租户回调接收端：第一次返回 500 以验证重试
	var mu sync.Mutex
	var got []hook
	calls := 0
	recv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			w.WriteHeader(500)
			return
		}
		got = append(got, hook{r.Header.Get("X-Kefu-Event-Type"), r.Header.Get("X-Kefu-Event-Id"), r.Header.Get("X-Kefu-Signature"), r.Header.Get("X-Kefu-Timestamp"), b})
	}))
	defer recv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wk := webhook.New(st, true)
	wk.Poll, wk.Backoff = 50*time.Millisecond, []time.Duration{100 * time.Millisecond}
	go wk.Run(ctx)

	var tenant store.Tenant
	do(t, "POST", srv.URL+"/v1/tenants", map[string]string{"X-Admin-Token": "adm"}, map[string]string{"name": "hook"}, &tenant)
	h := map[string]string{"X-App-Key": tenant.AppKey, "X-App-Secret": tenant.AppSecret}

	// 配置校验：非法/内网（此处 API 允许内网，故只测非法 scheme）
	if c := do(t, "PUT", srv.URL+"/v1/webhook", h, map[string]string{"url": "ftp://x"}, nil); c != 400 {
		t.Fatalf("bad url should be 400, got %d", c)
	}
	var wh store.Webhook
	if c := do(t, "PUT", srv.URL+"/v1/webhook", h, map[string]string{"url": recv.URL}, &wh); c != 200 || wh.Secret == "" {
		t.Fatalf("put webhook: %d %+v", c, wh)
	}

	var visitor store.Account
	do(t, "POST", srv.URL+"/v1/visitors/register", nil, map[string]string{"app_key": tenant.AppKey}, &visitor)

	// 服务端发消息：按访客 accid 自动开会话；client_msg_id 幂等
	var m1, m2 store.Message
	body := map[string]any{"to": visitor.Accid, "body": "您的订单已发货", "client_msg_id": "order-1"}
	if c := do(t, "POST", srv.URL+"/v1/messages", h, body, &m1); c != 200 {
		t.Fatalf("send: %d", c)
	}
	do(t, "POST", srv.URL+"/v1/messages", h, body, &m2)
	if m1.ID != m2.ID || m1.Seq != m2.Seq {
		t.Fatalf("not idempotent: %+v %+v", m1, m2)
	}
	if c := do(t, "POST", srv.URL+"/v1/messages", h, map[string]any{"body": "x"}, nil); c != 400 {
		t.Fatalf("missing target should be 400, got %d", c)
	}
	if c := do(t, "POST", srv.URL+"/v1/messages", h, map[string]any{"to": "nobody", "body": "x"}, nil); c != 404 {
		t.Fatalf("unknown visitor should be 404, got %d", c)
	}

	// 最近会话：访客未读 1；标记已读后归零
	var rec []store.RecentItem
	do(t, "GET", srv.URL+"/v1/accounts/"+visitor.Accid+"/recent", h, nil, &rec)
	if len(rec) != 1 || rec[0].Unread != 1 || rec[0].LastMsg != "您的订单已发货" {
		t.Fatalf("recent: %+v", rec)
	}
	vc := dial(t, srv.URL, tenant.AppKey, visitor)
	defer vc.Close()
	vc.WriteJSON(map[string]any{"op": "read", "session_id": m1.SessionID, "seq": 999}) // 超过最新 seq 会被截断
	wait(t, vc, func(e im.Event) bool { return e.Op == "read" && e.Seq == m1.Seq })
	vc.WriteJSON(map[string]any{"op": "recent"})
	ev := wait(t, vc, func(e im.Event) bool { return e.Op == "recent" })
	if len(ev.Recent) != 1 || ev.Recent[0].Unread != 0 {
		t.Fatalf("after read: %+v", ev.Recent)
	}

	// 回调：session.created + message.created，签名可验证，且经过一次重试
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 3 || time.Now().After(deadline) { // session.created + message.created + presence.changed
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls < 4 || len(got) < 3 {
		t.Fatalf("calls=%d got=%d", calls, len(got))
	}
	types := map[string]bool{}
	for _, g := range got {
		types[g.Type] = true
		if g.Sig != webhook.Sign(wh.Secret, g.TS, g.Body) {
			t.Fatalf("bad signature for %s", g.Type)
		}
		var env struct {
			EventID int64           `json:"event_id"`
			Data    json.RawMessage `json:"data"`
		}
		if json.Unmarshal(g.Body, &env) != nil || env.EventID == 0 || len(env.Data) == 0 {
			t.Fatalf("bad envelope: %s", g.Body)
		}
	}
	if !types["session.created"] || !types["message.created"] {
		t.Fatalf("types: %v", types)
	}

	// 关闭回调后不再入队
	do(t, "DELETE", srv.URL+"/v1/webhook", h, nil, nil)
	if w, _ := st.GetWebhook(context.Background(), tenant.ID); w.URL != "" {
		t.Fatal("webhook not removed")
	}
}

func TestWebhookURLValidation(t *testing.T) {
	for _, u := range []string{"http://127.0.0.1/x", "http://localhost/x", "http://10.0.0.1/", "http://169.254.169.254/latest", "http://[::1]/", "http://100.64.0.1/", "ftp://example.com", "http://u:p@example.com/", "not a url"} {
		if webhook.ValidateURL(u, false) == nil {
			t.Errorf("%q should be rejected", u)
		}
	}
	if err := webhook.ValidateURL("https://example.com/hook", false); err != nil {
		t.Error(err)
	}
}

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

// 拨号期拦截：即使回调地址通过了配置（如直接写库/域名解析到内网），worker 也不会连内网。
func TestWebhookDialBlocksPrivate(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN")
	}
	st, err := store.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	hit := make(chan struct{}, 1)
	recv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hit <- struct{}{} }))
	defer recv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tn, _ := st.CreateTenant(ctx, "ssrf")
	st.SetWebhook(ctx, tn.ID, recv.URL, "s")
	if err := st.EnqueueEvent(ctx, tn.ID, "message.created", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	wk := webhook.New(st, false) // 不允许内网
	wk.Poll, wk.Backoff = 50*time.Millisecond, []time.Duration{time.Hour}
	go wk.Run(ctx)
	select {
	case <-hit:
		t.Fatal("worker connected to a loopback address")
	case <-time.After(1500 * time.Millisecond):
	}
}
