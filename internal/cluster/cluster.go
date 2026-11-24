// Package cluster 抽象多实例部署所需的共享能力：连接注册表（在线状态）、跨节点消息投递、
// 分布式锁、一次性标记。Local 实现保持单进程行为；Redis 实现用于多实例。
package cluster

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ConnRef 标识一条 WebSocket 连接。
type ConnRef struct {
	Tenant int64
	Accid  string
	ConnID string
}

type Cluster interface {
	NodeID() string

	// Add/Remove 登记/注销连接。first/last 是**全局**语义：该账号此前没有任何有效连接 / 此后不再有任何连接。
	Add(ctx context.Context, tenant int64, accid, connID string) (first bool, err error)
	Remove(ctx context.Context, tenant int64, accid, connID string) (last bool, err error)
	// Members 返回账号当前有效连接，形如 "节点ID|连接ID"。
	Members(ctx context.Context, tenant int64, accid string) ([]string, error)

	// Publish 向指定节点投递一条消息。
	Publish(ctx context.Context, node string, payload []byte) error

	// Run 订阅本节点的消息并按 TTL 心跳续期本节点的连接，阻塞到 ctx 结束。
	Run(ctx context.Context, local func() []ConnRef, onMessage func(payload []byte))

	// Lock 尝试获取互斥锁（非阻塞）。ttl 兜底防止持有者崩溃后死锁。
	Lock(ctx context.Context, key string, ttl time.Duration) (unlock func(), ok bool)
	// Once 在 ttl 内只对第一个调用者返回 true（跨节点限流/抢占）。
	Once(ctx context.Context, key string, ttl time.Duration) bool
	// Announce 设置/清除全局标记，返回标记是否发生了变化（用于在线/离线事件去重）。
	Announce(ctx context.Context, key string, on bool) (changed bool)

	Close() error
}

// NodeOf 从 Members 项中取出节点 ID。
func NodeOf(member string) string {
	if i := strings.IndexByte(member, '|'); i >= 0 {
		return member[:i]
	}
	return member
}

// ---------------- Local：单进程实现 ----------------

type Local struct {
	mu    sync.Mutex
	conns map[string]map[string]struct{}
	locks map[string]time.Time
	once  map[string]time.Time
	ann   map[string]bool
}

func NewLocal() *Local {
	return &Local{conns: map[string]map[string]struct{}{}, locks: map[string]time.Time{}, once: map[string]time.Time{}, ann: map[string]bool{}}
}

func (l *Local) NodeID() string { return "local" }

func ck(t int64, accid string) string { return strconv.FormatInt(t, 10) + "/" + accid }

func (l *Local) Add(_ context.Context, t int64, accid, id string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	k := ck(t, accid)
	if l.conns[k] == nil {
		l.conns[k] = map[string]struct{}{}
	}
	first := len(l.conns[k]) == 0
	l.conns[k][id] = struct{}{}
	return first, nil
}

func (l *Local) Remove(_ context.Context, t int64, accid, id string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	k := ck(t, accid)
	delete(l.conns[k], id)
	if len(l.conns[k]) == 0 {
		delete(l.conns, k)
		return true, nil
	}
	return false, nil
}

func (l *Local) Members(_ context.Context, t int64, accid string) ([]string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for id := range l.conns[ck(t, accid)] {
		out = append(out, "local|"+id)
	}
	return out, nil
}

func (l *Local) Publish(context.Context, string, []byte) error { return nil }

func (l *Local) Run(ctx context.Context, _ func() []ConnRef, _ func([]byte)) { <-ctx.Done() }

func (l *Local) Lock(_ context.Context, key string, ttl time.Duration) (func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if exp, ok := l.locks[key]; ok && time.Now().Before(exp) {
		return nil, false
	}
	exp := time.Now().Add(ttl)
	l.locks[key] = exp
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.locks[key].Equal(exp) {
			delete(l.locks, key)
		}
	}, true
}

func (l *Local) Once(_ context.Context, key string, ttl time.Duration) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if exp, ok := l.once[key]; ok && time.Now().Before(exp) {
		return false
	}
	if len(l.once) > 50000 { // 清理过期项，防止无限增长
		for k, e := range l.once {
			if time.Now().After(e) {
				delete(l.once, k)
			}
		}
	}
	l.once[key] = time.Now().Add(ttl)
	return true
}

func (l *Local) Announce(_ context.Context, key string, on bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ann[key] == on {
		return false
	}
	if on {
		l.ann[key] = true
	} else {
		delete(l.ann, key)
	}
	return true
}

func (l *Local) Close() error { return nil }
