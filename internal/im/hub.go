package im

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

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
}

type Conn struct {
	ws   *websocket.Conn
	acc  *store.Account
	send chan []byte
}

type Hub struct {
	mu    sync.RWMutex
	conns map[string]map[*Conn]struct{} // key: tenantID/accid（同账号多端在线）
}

func NewHub() *Hub { return &Hub{conns: map[string]map[*Conn]struct{}{}} }

func key(t int64, accid string) string { return fmt.Sprintf("%d/%s", t, accid) }

// add 注册连接，返回是否为该账号的第一个连接。
func (h *Hub) add(c *Conn) (first bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	k := key(c.acc.TenantID, c.acc.Accid)
	if h.conns[k] == nil {
		h.conns[k] = map[*Conn]struct{}{}
	}
	first = len(h.conns[k]) == 0
	h.conns[k][c] = struct{}{}
	return first
}

// remove 注销连接，返回是否已是该账号的最后一个连接。
func (h *Hub) remove(c *Conn) (last bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	k := key(c.acc.TenantID, c.acc.Accid)
	delete(h.conns[k], c)
	if len(h.conns[k]) == 0 {
		delete(h.conns, k)
		return true
	}
	return false
}

// Count 返回账号当前在线连接数（多端同时在线）。
func (h *Hub) Count(t int64, accid string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.conns[key(t, accid)])
}

func (h *Hub) Online(t int64, accid string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.conns[key(t, accid)]) > 0
}

// Push 向账号的所有在线端推送；离线则靠 Sync 补拉。
func (h *Hub) Push(t int64, accid string, ev Event) {
	b, _ := json.Marshal(ev)
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.conns[key(t, accid)] {
		select {
		case c.send <- b:
		default: // 慢连接：丢弃，客户端靠 seq 缺口同步
		}
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
		if !errors.Is(err, ErrForbidden) && !errors.Is(err, ErrBadInput) && !errors.Is(err, ErrClosed) && !errors.Is(err, ErrSensitive) && !errors.Is(err, store.ErrNotFound) {
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
