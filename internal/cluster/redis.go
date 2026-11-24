package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis 实现。
//
// 在线状态：每个账号一个 ZSET  kefu:p:{tenant}:{accid}，member="节点|连接ID"，score=过期时间(毫秒)。
// 时间一律取 Redis 服务器的 TIME，避免各节点时钟偏差。节点每 TTL/3 心跳续期自己的连接；
// 节点崩溃后其连接最多 TTL 后自动失效。
// 跨节点投递：每个节点订阅 kefu:n:{节点ID}，发送方按账号的 Members 把消息发给持有连接的节点。
type Redis struct {
	rdb     *redis.Client
	node    string
	ConnTTL time.Duration
}

// NewRedis 连接 Redis。nodeID 为空时自动生成（主机名-随机串）；每个进程必须唯一。
func NewRedis(opt *redis.Options, nodeID string) (*Redis, error) {
	rdb := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}
	if nodeID == "" {
		h, _ := os.Hostname()
		b := make([]byte, 4)
		_, _ = rand.Read(b)
		nodeID = h + "-" + hex.EncodeToString(b)
	}
	return &Redis{rdb: rdb, node: nodeID, ConnTTL: 30 * time.Second}, nil
}

func (r *Redis) NodeID() string                 { return r.node }
func (r *Redis) Close() error                   { return r.rdb.Close() }
func (r *Redis) Ping(ctx context.Context) error { return r.rdb.Ping(ctx).Err() }

func pkey(t int64, accid string) string      { return "kefu:p:" + strconv.FormatInt(t, 10) + ":" + accid }
func (r *Redis) member(connID string) string { return r.node + "|" + connID }

const nowMs = `local t=redis.call('TIME'); local now=t[1]*1000+math.floor(t[2]/1000);`

var (
	addScript = redis.NewScript(nowMs + `
redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',now)
local before=redis.call('ZCARD',KEYS[1])
redis.call('ZADD',KEYS[1],now+ARGV[2],ARGV[1])
redis.call('PEXPIRE',KEYS[1],ARGV[2]*2)
return before`)
	removeScript = redis.NewScript(nowMs + `
redis.call('ZREM',KEYS[1],ARGV[1])
redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',now)
return redis.call('ZCARD',KEYS[1])`)
	membersScript = redis.NewScript(nowMs + `
return redis.call('ZRANGEBYSCORE',KEYS[1],'('..now,'+inf')`)
	// 心跳：ARGV[1]=ttl(ms)，ARGV[2..]=成员。用 ZADD 而非 XX：短暂失联后过期被清理的连接会重新登记。
	touchScript = redis.NewScript(nowMs + `
for i=2,#ARGV do redis.call('ZADD',KEYS[1],now+ARGV[1],ARGV[i]) end
redis.call('PEXPIRE',KEYS[1],ARGV[1]*2)
return 1`)
	unlockScript = redis.NewScript(`
if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end
return 0`)
)

func (r *Redis) Add(ctx context.Context, t int64, accid, id string) (bool, error) {
	n, err := addScript.Run(ctx, r.rdb, []string{pkey(t, accid)}, r.member(id), r.ConnTTL.Milliseconds()).Int()
	return n == 0, err
}

func (r *Redis) Remove(ctx context.Context, t int64, accid, id string) (bool, error) {
	n, err := removeScript.Run(ctx, r.rdb, []string{pkey(t, accid)}, r.member(id)).Int()
	return n == 0, err
}

func (r *Redis) Members(ctx context.Context, t int64, accid string) ([]string, error) {
	v, err := membersScript.Run(ctx, r.rdb, []string{pkey(t, accid)}).StringSlice()
	if err == redis.Nil {
		return nil, nil
	}
	return v, err
}

func (r *Redis) channel(node string) string { return "kefu:n:" + node }

func (r *Redis) Publish(ctx context.Context, node string, payload []byte) error {
	return r.rdb.Publish(ctx, r.channel(node), payload).Err()
}

func (r *Redis) Run(ctx context.Context, local func() []ConnRef, onMessage func([]byte)) {
	sub := r.rdb.Subscribe(ctx, r.channel(r.node))
	defer sub.Close()
	go func() { // 订阅：go-redis 会在断线后自动重连并重新订阅
		for m := range sub.Channel() {
			onMessage([]byte(m.Payload))
		}
	}()
	tick := time.NewTicker(r.ConnTTL / 3)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			r.heartbeat(ctx, local())
		}
	}
}

// heartbeat 按账号分组、用 pipeline 一次续期本节点的所有连接。
func (r *Redis) heartbeat(ctx context.Context, conns []ConnRef) {
	if len(conns) == 0 {
		return
	}
	type k struct {
		t int64
		a string
	}
	groups := map[k][]any{}
	for _, c := range conns {
		kk := k{c.Tenant, c.Accid}
		if groups[kk] == nil {
			groups[kk] = []any{r.ConnTTL.Milliseconds()}
		}
		groups[kk] = append(groups[kk], r.member(c.ConnID))
	}
	pipe := r.rdb.Pipeline()
	for kk, args := range groups {
		touchScript.Eval(ctx, pipe, []string{pkey(kk.t, kk.a)}, args...)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("cluster heartbeat: %v", err)
	}
}

func (r *Redis) Lock(ctx context.Context, key string, ttl time.Duration) (func(), bool) {
	k := "kefu:lock:" + key
	ok, err := r.rdb.SetNX(ctx, k, r.node, ttl).Result()
	if err != nil || !ok {
		return nil, false
	}
	return func() { _ = unlockScript.Run(context.Background(), r.rdb, []string{k}, r.node).Err() }, true
}

func (r *Redis) Once(ctx context.Context, key string, ttl time.Duration) bool {
	ok, err := r.rdb.SetNX(ctx, "kefu:once:"+key, 1, ttl).Result()
	if err != nil {
		return true // Redis 异常时宁可多推一次，也不丢通知
	}
	return ok
}

const announceTTL = 24 * time.Hour // 兜底：持有标记的节点崩溃时不至于永久"在线"

func (r *Redis) Announce(ctx context.Context, key string, on bool) bool {
	k := "kefu:ann:" + key
	if on {
		ok, err := r.rdb.SetNX(ctx, k, 1, announceTTL).Result()
		return err == nil && ok
	}
	n, err := r.rdb.Del(ctx, k).Result()
	return err == nil && n > 0
}
