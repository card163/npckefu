package im

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"npckefu/internal/cluster"
	"npckefu/internal/store"
)

// Event 是服务端 -> 客户端的推送帧。
type Event struct {
	Op          string             `json:"op"` // msg | session | queue | ack | typing | synced | error
	Message     *store.Message     `json:"message,omitempty"`
	Messages    []store.Message    `json:"messages,omitempty"`
	Recent      []store.RecentItem `json:"recent,omitempty"`
	Session     *store.Session     `json:"session,omitempty"`
	SessionID   int64              `json:"session_id,omitempty"`
	ClientMsgID string             `json:"client_msg_id,omitempty"`
	Seq         int64              `json:"seq,omitempty"`
	Position    int                `json:"position,omitempty"`
	Error       string             `json:"error,omitempty"`
	Score       int                `json:"score,omitempty"`
	Comment     string             `json:"comment,omitempty"`
	From        string             `json:"from,omitempty"`
	Note        string             `json:"note,omitempty"`
	Agents      []RosterItem       `json:"agents,omitempty"`
}

type Conn struct {
	id   string
	ws   *websocket.Conn
	acc  *store.Account
	send chan []byte
}

// Hub 管理本进程的连接，并借助 cluster 提供全局在线状态与跨节点投递。
type Hub struct {
	cl    cluster.Cluster
	mu    sync.RWMutex
	conns map[string]map[*Conn]struct{} // key: tenantID/accid（同账号多端在线）
}

func NewHub(cl cluster.Cluster) *Hub {
	return &Hub{cl: cl, conns: map[string]map[*Conn]struct{}{}}
}

func key(t int64, accid string) string { return fmt.Sprintf("%d/%s", t, accid) }

func opCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}

// add 注册连接，返回该账号在**全局**范围内是否第一次上线。
func (h *Hub) add(c *Conn) (first bool) {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	c.id = hex.EncodeToString(b)
	h.mu.Lock()
	k := key(c.acc.TenantID, c.acc.Accid)
	if h.conns[k] == nil {
		h.conns[k] = map[*Conn]struct{}{}
	}
	localFirst := len(h.conns[k]) == 0
	h.conns[k][c] = struct{}{}
	h.mu.Unlock()
	ctx, cancel := opCtx()
	defer cancel()
	first, err := h.cl.Add(ctx, c.acc.TenantID, c.acc.Accid, c.id)
	if err != nil {
		log.Printf("cluster add: %v", err)
		return localFirst
	}
	return first
}

// remove 注销连接，返回该账号是否已在全局范围内下线。
func (h *Hub) remove(c *Conn) (last bool) {
	h.mu.Lock()
	k := key(c.acc.TenantID, c.acc.Accid)
	delete(h.conns[k], c)
	localLast := len(h.conns[k]) == 0
	if localLast {
		delete(h.conns, k)
	}
	h.mu.Unlock()
	ctx, cancel := opCtx()
	defer cancel()
	last, err := h.cl.Remove(ctx, c.acc.TenantID, c.acc.Accid, c.id)
	if err != nil {
		log.Printf("cluster remove: %v", err)
		return localLast
	}
	return last
}

func (h *Hub) hasLocal(t int64, accid string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.conns[key(t, accid)]) > 0
}

// Count 返回账号在所有节点上的在线连接数（多端同时在线）。
func (h *Hub) Count(t int64, accid string) int {
	ctx, cancel := opCtx()
	defer cancel()
	m, err := h.cl.Members(ctx, t, accid)
	if err != nil {
		log.Printf("cluster members: %v", err)
		h.mu.RLock()
		defer h.mu.RUnlock()
		return len(h.conns[key(t, accid)])
	}
	return len(m)
}

// Online 账号在任一节点有连接即为在线。本地有连接时无需访问共享存储。
func (h *Hub) Online(t int64, accid string) bool {
	return h.hasLocal(t, accid) || h.Count(t, accid) > 0
}

// pushLocal 投递给本进程上的该账号连接。
func (h *Hub) pushLocal(t int64, accid string, b []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.conns[key(t, accid)] {
		select {
		case c.send <- b:
		default: // 慢连接：丢弃，客户端靠 seq 缺口同步
		}
	}
}

type remoteEnvelope struct {
	T int64           `json:"t"`
	A string          `json:"a"`
	E json.RawMessage `json:"e"`
}

// Push 向账号的所有在线端推送（含其他节点上的连接）；离线则靠 Sync 补拉。
func (h *Hub) Push(t int64, accid string, ev Event) {
	b, _ := json.Marshal(ev)
	h.pushLocal(t, accid, b)
	ctx, cancel := opCtx()
	defer cancel()
	members, err := h.cl.Members(ctx, t, accid)
	if err != nil {
		log.Printf("cluster members: %v", err)
		return
	}
	self, sent := h.cl.NodeID(), map[string]bool{}
	var env []byte
	for _, m := range members {
		n := cluster.NodeOf(m)
		if n == self || sent[n] {
			continue
		}
		sent[n] = true
		if env == nil {
			env, _ = json.Marshal(remoteEnvelope{T: t, A: accid, E: b})
		}
		if err := h.cl.Publish(ctx, n, env); err != nil {
			log.Printf("cluster publish to %s: %v", n, err)
		}
	}
}

func (h *Hub) onRemote(payload []byte) {
	var e remoteEnvelope
	if json.Unmarshal(payload, &e) == nil {
		h.pushLocal(e.T, e.A, e.E)
	}
}

func (h *Hub) snapshot() []cluster.ConnRef {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var out []cluster.ConnRef
	for _, set := range h.conns {
		for c := range set {
			out = append(out, cluster.ConnRef{Tenant: c.acc.TenantID, Accid: c.acc.Accid, ConnID: c.id})
		}
	}
	return out
}

// Run 运行集群后台任务（订阅跨节点消息、连接心跳），阻塞到 ctx 结束。
func (h *Hub) Run(ctx context.Context) { h.cl.Run(ctx, h.snapshot, h.onRemote) }

// CloseAll 关闭本进程所有连接（优雅停机）：客户端会自动重连到其他节点。
func (h *Hub) CloseAll() {
	h.mu.RLock()
	var all []*Conn
	for _, set := range h.conns {
		for c := range set {
			all = append(all, c)
		}
	}
	h.mu.RUnlock()
	for _, c := range all {
		_ = c.ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "server restarting"), time.Now().Add(time.Second))
		_ = c.ws.Close()
	}
}

// ---- WebSocket 网关 ----

var upgrader = websocket.Upgrader{
	ReadBufferSize: 4096, WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool { return true }, // 生产环境请按租户配置的域名白名单校验
}

type inbound struct {
	Op string `json:"op"` // start | send | sync | close | typing | ping
	SendReq
	SkillGroup    string `json:"skill_group"`
	Since         int64  `json:"since_seq"`
	Seq           int64  `json:"seq"`
	IncludeClosed bool   `json:"include_closed"`
	Platform      string `json:"platform"`
	Token         string `json:"token"`
	Score         int    `json:"score"`
	Comment       string `json:"comment"`
	ToAgent       string `json:"to_agent"`
	Note          string `json:"note"`
}

// ServeWS 登录鉴权：?appkey=&accid=&token=（token 由租户服务端通过注册接口下发）。
func (s *Service) ServeWS(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	t, err := s.St.TenantByAppKey(r.Context(), q.Get("appkey"))
	if err != nil {
		http.Error(w, "invalid appkey", http.StatusUnauthorized)
		return
	}
	acc, err := s.St.Account(r.Context(), t.ID, q.Get("accid"))
	if err != nil || subtle.ConstantTimeCompare([]byte(acc.Token), []byte(q.Get("token"))) != 1 {
		http.Error(w, "invalid account or token", http.StatusUnauthorized)
		return
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &Conn{ws: ws, acc: acc, send: make(chan []byte, 128)}
	if s.Hub.add(c) {
		s.presenceUp(acc)
	}
	go c.writeLoop()
	if acc.Role == store.RoleAgent {
		go s.Dispatch(context.Background(), acc.TenantID) // 坐席上线，消化排队
	}
	if acc.Role == store.RoleVisitor {
		if sess, err := s.St.OpenSessionOfVisitor(r.Context(), acc.TenantID, acc.Accid); err == nil {
			s.Hub.Push(acc.TenantID, acc.Accid, Event{Op: "session", Session: sess})
		}
	}
	if acc.Role == store.RoleAgent { // 坐席重连：下发进行中的会话
		if list, err := s.St.ListSessions(r.Context(), acc.TenantID, store.StatusActive, acc.Accid, 200); err == nil {
			for i := range list {
				s.Hub.Push(acc.TenantID, acc.Accid, Event{Op: "session", Session: &list[i]})
			}
		}
	}
	s.readLoop(c)
	if s.Hub.remove(c) {
		s.presenceDown(acc)
	}
	close(c.send)
	ws.Close()
}

func (c *Conn) writeLoop() {
	tick := time.NewTicker(25 * time.Second)
	defer tick.Stop()
	for {
		select {
		case b, ok := <-c.send:
			if !ok {
				return
			}
			_ = c.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if c.ws.WriteMessage(websocket.TextMessage, b) != nil {
				return
			}
		case <-tick.C:
			_ = c.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if c.ws.WriteMessage(websocket.PingMessage, nil) != nil {
				return
			}
		}
	}
}

func (s *Service) readLoop(c *Conn) {
	c.ws.SetReadLimit(64 << 10)
	_ = c.ws.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.ws.SetPongHandler(func(string) error { return c.ws.SetReadDeadline(time.Now().Add(60 * time.Second)) })
	for {
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			return
		}
		_ = c.ws.SetReadDeadline(time.Now().Add(60 * time.Second))
		var in inbound
		if err := json.Unmarshal(data, &in); err != nil {
			s.Hub.Push(c.acc.TenantID, c.acc.Accid, Event{Op: "error", Error: "bad json"})
			continue
		}
		s.handle(c, in)
	}
}

func (s *Service) handle(c *Conn, in inbound) {
	ctx := context.Background()
	reply := func(ev Event) {
		b, _ := json.Marshal(ev)
		select {
		case c.send <- b:
		default:
		}
	}
	fail := func(err error) {
		if !isUserError(err) {
			log.Printf("ws op=%s: %v", in.Op, err)
			err = errors.New("internal error")
		}
		reply(Event{Op: "error", Error: err.Error(), SessionID: in.SessionID, ClientMsgID: in.ClientMsgID})
	}
	switch in.Op {
	case "ping":
	case "start":
		sess, err := s.StartSession(ctx, c.acc, in.SkillGroup)
		if err != nil {
			fail(err)
			return
		}
		s.Hub.Push(c.acc.TenantID, c.acc.Accid, Event{Op: "session", Session: sess})
	case "send":
		m, err := s.Send(ctx, c.acc, in.SendReq)
		if err != nil {
			fail(err)
			return
		}
		reply(Event{Op: "ack", SessionID: m.SessionID, ClientMsgID: m.ClientMsgID, Seq: m.Seq, Message: m})
	case "sync":
		msgs, err := s.Sync(ctx, c.acc, in.SessionID, in.Since)
		if err != nil {
			fail(err)
			return
		}
		reply(Event{Op: "synced", SessionID: in.SessionID, Messages: msgs})
	case "recent":
		items, err := s.Recent(ctx, c.acc, in.IncludeClosed, 100)
		if err != nil {
			fail(err)
			return
		}
		reply(Event{Op: "recent", Recent: items})
	case "agents": // 坐席查看同事名单（在线/负载），用于选择转接目标
		if c.acc.Role != store.RoleAgent {
			fail(ErrForbidden)
			return
		}
		r, err := s.Roster(ctx, c.acc.TenantID)
		if err != nil {
			fail(err)
			return
		}
		reply(Event{Op: "agents", Agents: r})
	case "transfer": // 坐席把自己的会话转给他人/技能组/退回队列
		if c.acc.Role != store.RoleAgent {
			fail(ErrForbidden)
			return
		}
		if _, err := s.TransferAgent(ctx, c.acc.TenantID, c.acc.Accid, in.SessionID, TransferReq{ToAgent: in.ToAgent, SkillGroup: in.SkillGroup, Note: in.Note}); err != nil {
			fail(err)
			return
		}
		reply(Event{Op: "transfer_ok", SessionID: in.SessionID})
	case "rate": // 访客评价已结束的会话
		r, err := s.Rate(ctx, c.acc, in.SessionID, in.Score, in.Comment)
		if err != nil {
			fail(err)
			return
		}
		reply(Event{Op: "rated", SessionID: r.SessionID, Score: r.Score})
	case "device": // 客户端登记/更新自己的推送 token
		if err := s.RegisterDevice(ctx, c.acc, in.Platform, in.Token); err != nil {
			fail(err)
		}
	case "read":
		if err := s.MarkRead(ctx, c.acc, in.SessionID, in.Seq); err != nil {
			fail(err)
		}
	case "close":
		if err := s.Close(ctx, c.acc, in.SessionID); err != nil {
			fail(err)
		}
	case "typing":
		sess, err := s.St.Session(ctx, c.acc.TenantID, in.SessionID)
		if err != nil {
			return
		}
		peer := sess.AgentAccid
		if c.acc.Accid == sess.AgentAccid {
			peer = sess.VisitorAccid
		} else if c.acc.Accid != sess.VisitorAccid {
			return
		}
		if peer != "" {
			s.Hub.Push(c.acc.TenantID, peer, Event{Op: "typing", SessionID: sess.ID})
		}
	default:
		fail(ErrBadInput)
	}
}
