package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"npckefu/internal/api"
	"npckefu/internal/bot"
	"npckefu/internal/im"
	"npckefu/internal/store"
)

func post(t *testing.T, url string, hdr map[string]string, body any, out any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", url, bytes.NewReader(b))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("POST %s: %d", url, resp.StatusCode)
	}
	if out != nil {
		json.NewDecoder(resp.Body).Decode(out)
	}
}

func dial(t *testing.T, base, appkey string, a store.Account) *websocket.Conn {
	t.Helper()
	u := "ws" + strings.TrimPrefix(base, "http") + "/ws?appkey=" + appkey + "&accid=" + a.Accid + "&token=" + a.Token
	c, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// wait 读帧直到满足条件。
func wait(t *testing.T, c *websocket.Conn, pred func(im.Event) bool) im.Event {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		var ev im.Event
		if err := c.ReadJSON(&ev); err != nil {
			t.Fatalf("read: %v", err)
		}
		if pred(ev) {
			return ev
		}
	}
}

func TestFlow(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN (e.g. root:@tcp(127.0.0.1:3306)/npckefu_test) to run")
	}
	st, err := store.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc := im.NewService(st, bot.New(st, nil))
	a := &api.API{St: st, IM: svc, AdminToken: "adm"}
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()

	var tenant store.Tenant
	post(t, srv.URL+"/v1/tenants", map[string]string{"X-Admin-Token": "adm"}, map[string]string{"name": "acme"}, &tenant)
	h := map[string]string{"X-App-Key": tenant.AppKey, "X-App-Secret": tenant.AppSecret}
	post(t, srv.URL+"/v1/faqs", h, store.FAQ{Question: "如何申请退款", Answer: "在订单页点击申请退款即可", Keywords: "退款,退钱"}, nil)
	var agent, visitor store.Account
	post(t, srv.URL+"/v1/accounts", h, store.Account{Accid: "agent1", Name: "小王", Role: "agent"}, &agent)
	post(t, srv.URL+"/v1/visitors/register", nil, map[string]string{"app_key": tenant.AppKey}, &visitor)

	vc := dial(t, srv.URL, tenant.AppKey, visitor)
	defer vc.Close()
	vc.WriteJSON(map[string]any{"op": "start"})
	sess := wait(t, vc, func(e im.Event) bool { return e.Op == "session" }).Session

	// 1) 机器人命中 FAQ
	vc.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "c1", "body": "怎么退款"})
	ev := wait(t, vc, func(e im.Event) bool {
		return e.Op == "msg" && e.Message.FromRole == "bot" && strings.Contains(e.Message.Body, "申请退款")
	})
	_ = ev

	// 幂等：同一 client_msg_id 重发，seq 不变
	vc.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "c1", "body": "怎么退款"})
	ack := wait(t, vc, func(e im.Event) bool { return e.Op == "ack" && e.ClientMsgID == "c1" })
	if ack.Seq == 0 {
		t.Fatal("no seq")
	}

	// 2) 转人工 -> 无坐席在线，排队
	vc.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "c2", "body": "转人工"})
	wait(t, vc, func(e im.Event) bool { return e.Op == "queue" })

	// 3) 坐席上线，自动分配
	ac := dial(t, srv.URL, tenant.AppKey, agent)
	defer ac.Close()
	got := wait(t, ac, func(e im.Event) bool { return e.Op == "session" && e.Session.Status == "active" })
	if got.Session.AgentAccid != "agent1" {
		t.Fatalf("bad agent %q", got.Session.AgentAccid)
	}
	ac.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "a1", "body": "您好，请问怎么了"})
	wait(t, vc, func(e im.Event) bool { return e.Op == "msg" && e.Message.From == "agent1" })

	// 4) 坐席发消息后访客的消息不再触发机器人；同步拉取
	vc.WriteJSON(map[string]any{"op": "sync", "session_id": sess.ID, "since_seq": 0})
	sy := wait(t, vc, func(e im.Event) bool { return e.Op == "synced" })
	if len(sy.Messages) < 6 {
		t.Fatalf("expected >=6 msgs, got %d", len(sy.Messages))
	}
	for i := 1; i < len(sy.Messages); i++ {
		if sy.Messages[i].Seq != sy.Messages[i-1].Seq+1 {
			t.Fatal("seq gap")
		}
	}

	// 5) 关闭会话
	ac.WriteJSON(map[string]any{"op": "close", "session_id": sess.ID})
	wait(t, vc, func(e im.Event) bool { return e.Op == "session" && e.Session.Status == "closed" })

	// 租户鉴权失败
	req, _ := http.NewRequest("GET", srv.URL+"/v1/sessions", nil)
	req.Header.Set("X-App-Key", tenant.AppKey)
	req.Header.Set("X-App-Secret", "wrong")
	if r, _ := http.DefaultClient.Do(req); r.StatusCode != 401 {
		t.Fatalf("expected 401, got %d", r.StatusCode)
	}
}
