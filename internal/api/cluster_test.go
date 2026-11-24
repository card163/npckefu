package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"npckefu/internal/api"
	"npckefu/internal/bot"
	"npckefu/internal/cluster"
	"npckefu/internal/im"
	"npckefu/internal/store"
	"npckefu/internal/webhook"
)

// 多节点测试：多个应用节点共享同一个 MySQL 和 Redis，模拟真实的多实例部署。需要 TEST_MYSQL_DSN 与 TEST_REDIS_ADDR。

type node struct {
	srv    *httptest.Server
	svc    *im.Service
	cancel context.CancelFunc // 停止本节点后台任务（心跳/订阅）——相当于进程崩溃（连接不会被注销）
}

type clusterEnv struct {
	st     *store.Store
	addr   string
	tenant store.Tenant
	h      map[string]string
	mu     sync.Mutex
	events []map[string]any
	nodes  []*node
}

func newClusterEnv(t *testing.T) *clusterEnv {
	t.Helper()
	dsn, addr := os.Getenv("TEST_MYSQL_DSN"), os.Getenv("TEST_REDIS_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("set TEST_MYSQL_DSN and TEST_REDIS_ADDR")
	}
	st, err := store.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	flush := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	flush.FlushDB(context.Background())
	flush.Close()

	e := &clusterEnv{st: st, addr: addr}
	recv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		json.Unmarshal(b, &m)
		e.mu.Lock()
		e.events = append(e.events, m)
		e.mu.Unlock()
	}))
	t.Cleanup(recv.Close)
	wctx, wcancel := context.WithCancel(context.Background())
	t.Cleanup(wcancel)
	wk := webhook.New(st, true)
	wk.Poll = 50 * time.Millisecond
	go wk.Run(wctx)

	n := e.addNode(t, "node-a", 30*time.Second)
	do(t, "POST", n.srv.URL+"/v1/tenants", map[string]string{"X-Admin-Token": "adm"}, map[string]string{"name": "cluster"}, &e.tenant)
	e.h = map[string]string{"X-App-Key": e.tenant.AppKey, "X-App-Secret": e.tenant.AppSecret}
	do(t, "PUT", n.srv.URL+"/v1/webhook", e.h, map[string]string{"url": recv.URL}, nil)
	return e
}

func (e *clusterEnv) addNode(t *testing.T, id string, connTTL time.Duration) *node {
	t.Helper()
	rc, err := cluster.NewRedis(&redis.Options{Addr: e.addr, DB: 15}, id)
	if err != nil {
		t.Fatal(err)
	}
	rc.ConnTTL = connTTL
	svc := im.NewService(e.st, bot.New(e.st, nil), rc)
	svc.PresenceGrace, svc.SweepInterval, svc.PushInterval = 300*time.Millisecond, 200*time.Millisecond, time.Hour
	srv := httptest.NewServer((&api.API{St: e.st, IM: svc, AdminToken: "adm", AllowPrivateWebhook: true, Ready: []func(context.Context) error{e.st.Ping, rc.Ping}}).Handler())
	ctx, cancel := context.WithCancel(context.Background())
	go svc.Run(ctx)
	n := &node{srv: srv, svc: svc, cancel: cancel}
	t.Cleanup(func() { cancel(); srv.CloseClientConnections(); srv.Close(); rc.Close() })
	e.nodes = append(e.nodes, n)
	return n
}

func (e *clusterEnv) visitor(t *testing.T) store.Account {
	var v store.Account
	do(t, "POST", e.nodes[0].srv.URL+"/v1/visitors/register", nil, map[string]string{"app_key": e.tenant.AppKey}, &v)
	return v
}

func (e *clusterEnv) agent(t *testing.T, accid string, maxLoad int) store.Account {
	var a store.Account
	do(t, "POST", e.nodes[0].srv.URL+"/v1/accounts", e.h, store.Account{Accid: accid, Name: accid, Role: "agent", MaxLoad: maxLoad}, &a)
	return a
}

func (e *clusterEnv) countEvents(typ, accid string, online bool) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, ev := range e.events {
		if ev["type"] != typ {
			continue
		}
		d := ev["data"].(map[string]any)
		if d["accid"] == accid && d["online"] == online {
			n++
		}
	}
	return n
}

func presenceVia(t *testing.T, n *node, e *clusterEnv, accid string) im.Presence {
	var p im.Presence
	do(t, "GET", n.srv.URL+"/v1/accounts/"+accid+"/presence", e.h, nil, &p)
	return p
}

// 访客在节点 A、坐席在节点 B：排队→分配→双向消息→输入中，全部跨节点。
func TestClusterCrossNodeChat(t *testing.T) {
	e := newClusterEnv(t)
	a, b := e.nodes[0], e.addNode(t, "node-b", 30*time.Second)
	ag, v := e.agent(t, "ag1", 5), e.visitor(t)

	vc := dial(t, a.srv.URL, e.tenant.AppKey, v)
	defer vc.Close()
	vc.WriteJSON(map[string]any{"op": "start"})
	sess := wait(t, vc, func(x im.Event) bool { return x.Op == "session" }).Session
	vc.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "c1", "body": "转人工"})
	wait(t, vc, func(x im.Event) bool { return x.Op == "queue" })

	// 坐席连到 B：B 触发分配，A 上的访客收到"已分配"推送
	ac := dial(t, b.srv.URL, e.tenant.AppKey, ag)
	defer ac.Close()
	wait(t, ac, func(x im.Event) bool { return x.Op == "session" && x.Session.Status == "active" })
	wait(t, vc, func(x im.Event) bool {
		return x.Op == "session" && x.Session.Status == "active" && x.Session.AgentAccid == "ag1"
	})

	// 在线状态跨节点可见
	if p := presenceVia(t, a, e, ag.Accid); !p.Online || p.Connections != 1 {
		t.Fatalf("agent presence via A: %+v", p)
	}
	if p := presenceVia(t, b, e, v.Accid); !p.Online || p.Connections != 1 {
		t.Fatalf("visitor presence via B: %+v", p)
	}

	// 坐席(B) → 访客(A)；访客(A) → 坐席(B)；输入中
	ac.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "a1", "body": "您好"})
	wait(t, vc, func(x im.Event) bool { return x.Op == "msg" && x.Message.From == "ag1" && x.Message.Body == "您好" })
	vc.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "c2", "body": "在吗"})
	wait(t, ac, func(x im.Event) bool { return x.Op == "msg" && x.Message.From == v.Accid && x.Message.Body == "在吗" })
	vc.WriteJSON(map[string]any{"op": "typing", "session_id": sess.ID})
	wait(t, ac, func(x im.Event) bool { return x.Op == "typing" })
	// 结束会话（坐席在 B 上结束，访客在 A 上收到）
	ac.WriteJSON(map[string]any{"op": "close", "session_id": sess.ID})
	wait(t, vc, func(x im.Event) bool { return x.Op == "session" && x.Session.Status == "closed" })
}

// 同一账号在两个节点各有一个连接：connections=2；关一个仍在线；presence 事件各只发一次。
func TestClusterPresenceAcrossNodes(t *testing.T) {
	e := newClusterEnv(t)
	a, b := e.nodes[0], e.addNode(t, "node-b", 30*time.Second)
	v := e.visitor(t)

	c1 := dial(t, a.srv.URL, e.tenant.AppKey, v)
	c2 := dial(t, b.srv.URL, e.tenant.AppKey, v)
	time.Sleep(300 * time.Millisecond)
	if p := presenceVia(t, a, e, v.Accid); !p.Online || p.Connections != 2 {
		t.Fatalf("want 2 conns: %+v", p)
	}
	c1.Close()
	time.Sleep(700 * time.Millisecond) // 超过宽限期
	if p := presenceVia(t, b, e, v.Accid); !p.Online || p.Connections != 1 {
		t.Fatalf("one node left: %+v", p)
	}
	c2.Close()
	time.Sleep(900 * time.Millisecond)
	p := presenceVia(t, a, e, v.Accid)
	if p.Online || p.LastSeenAt == nil {
		t.Fatalf("offline: %+v", p)
	}
	// 事件跨节点只发一次
	if on, off := e.countEvents("presence.changed", v.Accid, true), e.countEvents("presence.changed", v.Accid, false); on != 1 || off != 1 {
		t.Fatalf("events: online=%d offline=%d (want 1/1)", on, off)
	}
}

// 两个节点并发把多个访客转人工，只有一个坐席（上限 2）：不得超配。
func TestClusterNoOverAssignment(t *testing.T) {
	e := newClusterEnv(t)
	a, b := e.nodes[0], e.addNode(t, "node-b", 30*time.Second)
	ag := e.agent(t, "solo", 2)
	ac := dial(t, b.srv.URL, e.tenant.AppKey, ag)
	defer ac.Close()
	time.Sleep(200 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		v := e.visitor(t)
		base := a.srv.URL
		if i%2 == 1 {
			base = b.srv.URL
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := dial(t, base, e.tenant.AppKey, v)
			t.Cleanup(func() { c.Close() })
			c.WriteJSON(map[string]any{"op": "start"})
			s := wait(t, c, func(x im.Event) bool { return x.Op == "session" }).Session
			c.WriteJSON(map[string]any{"op": "send", "session_id": s.ID, "client_msg_id": "t", "body": "转人工"})
		}()
	}
	wg.Wait()
	time.Sleep(1500 * time.Millisecond) // 含若干次兜底扫描
	load, _ := e.st.ActiveLoad(context.Background(), e.tenant.ID)
	queued, _ := e.st.QueuedSessions(context.Background(), e.tenant.ID)
	if load["solo"] != 2 || len(queued) != 6 {
		t.Fatalf("active=%d queued=%d (want 2/6)", load["solo"], len(queued))
	}
}

// 分配锁被占用时（别的节点正在分配），入队的会话由兜底扫描补上，不会搁浅。
func TestClusterSweeperRescuesQueue(t *testing.T) {
	e := newClusterEnv(t)
	a, b := e.nodes[0], e.addNode(t, "node-b", 30*time.Second)
	ag := e.agent(t, "sweep-ag", 5)
	v := e.visitor(t)

	// 占住分配锁，模拟另一个节点正在分配
	unlock, ok := a.svc.Cl.Lock(context.Background(), fmt.Sprintf("dispatch:%d", e.tenant.ID), 5*time.Second)
	if !ok {
		t.Fatal("lock")
	}
	vc := dial(t, a.srv.URL, e.tenant.AppKey, v)
	defer vc.Close()
	vc.WriteJSON(map[string]any{"op": "start"})
	sess := wait(t, vc, func(x im.Event) bool { return x.Op == "session" }).Session
	ac := dial(t, b.srv.URL, e.tenant.AppKey, ag)
	defer ac.Close()
	vc.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "t", "body": "转人工"})
	wait(t, vc, func(x im.Event) bool {
		return x.Op == "msg" && x.Message.FromRole == "system" && x.Message.Body == "正在为您转接人工客服，请稍候…"
	})
	time.Sleep(1500 * time.Millisecond) // 直接 Dispatch 重试 1s 后放弃
	if s, _ := e.st.Session(context.Background(), e.tenant.ID, sess.ID); s.Status != "queued" {
		t.Fatalf("should still be queued while lock held, got %s", s.Status)
	}
	unlock()
	wait(t, ac, func(x im.Event) bool {
		return x.Op == "session" && x.Session.Status == "active" && x.Session.ID == sess.ID
	})
}

// 节点崩溃（心跳停止、连接未注销）：其连接在 TTL 后自动失效，不会永远显示在线。
func TestClusterCrashedNodeExpires(t *testing.T) {
	e := newClusterEnv(t)
	a := e.nodes[0]
	c := e.addNode(t, "node-crash", 900*time.Millisecond)
	v := e.visitor(t)
	vc := dial(t, c.srv.URL, e.tenant.AppKey, v)
	defer vc.Close()
	time.Sleep(300 * time.Millisecond)
	if p := presenceVia(t, a, e, v.Accid); !p.Online {
		t.Fatalf("should be online: %+v", p)
	}
	time.Sleep(1500 * time.Millisecond) // 心跳正常：超过 TTL 仍在线
	if p := presenceVia(t, a, e, v.Accid); !p.Online {
		t.Fatalf("heartbeat should keep it online: %+v", p)
	}
	c.cancel() // "崩溃"：停止心跳
	time.Sleep(1300 * time.Millisecond)
	if p := presenceVia(t, a, e, v.Accid); p.Online {
		t.Fatalf("should have expired: %+v", p)
	}
}

func TestReadyz(t *testing.T) {
	e := newClusterEnv(t)
	r, err := http.Get(e.nodes[0].srv.URL + "/readyz")
	if err != nil || r.StatusCode != 200 {
		t.Fatalf("readyz: %v %v", err, r)
	}
}
