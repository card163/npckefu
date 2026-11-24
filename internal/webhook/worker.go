// Package webhook 把发件箱里的事件（消息抄送、会话状态变化）签名后异步投递到租户服务器，失败按退避重试。
//
// 请求：POST <callback_url>，JSON {"event_id","type","created_at","data"}
// 头：  X-Kefu-Event-Id / X-Kefu-Event-Type / X-Kefu-Timestamp(秒) /
//
//	X-Kefu-Signature = "sha256=" + hex(HMAC_SHA256(secret, timestamp + "." + body))
//
// 租户返回 2xx 视为成功；否则重试。投递语义为"至少一次"，请用 event_id 去重；
// 同一租户的事件按 event_id 递增产生，但重试可能导致乱序，需要顺序时请按 event_id 排序。
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"syscall"
	"time"

	"npckefu/internal/store"
)

const MaxAttempts = 8

var DefaultBackoff = []time.Duration{10 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute, 30 * time.Minute, time.Hour, 3 * time.Hour}

type Worker struct {
	St           *store.Store
	AllowPrivate bool            // 仅开发/测试：允许回调到内网/回环地址
	Poll         time.Duration   // 轮询间隔，默认 1s
	Backoff      []time.Duration // 重试间隔，默认 DefaultBackoff
	client       *http.Client
}

func New(st *store.Store, allowPrivate bool) *Worker {
	w := &Worker{St: st, AllowPrivate: allowPrivate, Poll: time.Second, Backoff: DefaultBackoff}
	dialer := &net.Dialer{
		Timeout: 5 * time.Second,
		// Control 在 DNS 解析之后、真正建立连接之前被调用，拿到的是实际要连的 IP，
		// 因此能挡住"域名解析到内网"与 DNS rebinding。
		Control: func(network, address string, _ syscall.RawConn) error {
			if w.AllowPrivate {
				return nil
			}
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			if ip, err := netip.ParseAddr(host); err != nil || blocked(ip) {
				return fmt.Errorf("callback address %q not allowed", host)
			}
			return nil
		},
	}
	w.client = &http.Client{
		Timeout:       8 * time.Second,
		Transport:     &http.Transport{Proxy: nil, DialContext: dialer.DialContext, MaxIdleConnsPerHost: 4},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, // 不跟随重定向
	}
	return w
}

var cgnat = netip.MustParsePrefix("100.64.0.0/10")

func blocked(ip netip.Addr) bool {
	ip = ip.Unmap()
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() || ip.IsUnspecified() || cgnat.Contains(ip)
}

// ValidateURL 在配置时做静态校验；实际的网络层拦截在拨号时进行。
func ValidateURL(raw string, allowPrivate bool) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || len(raw) > 512 {
		return errors.New("url must be a valid http(s) URL")
	}
	if u.User != nil {
		return errors.New("url must not contain credentials")
	}
	if !allowPrivate {
		if ip, err := netip.ParseAddr(u.Hostname()); err == nil && blocked(ip) {
			return errors.New("private/loopback addresses are not allowed")
		}
		if u.Hostname() == "localhost" {
			return errors.New("private/loopback addresses are not allowed")
		}
	}
	return nil
}

// Sign 生成签名，租户侧用同样算法校验。
func Sign(secret, ts string, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(ts + "."))
	m.Write(body)
	return "sha256=" + hex.EncodeToString(m.Sum(nil))
}

func (w *Worker) Run(ctx context.Context) {
	tick := time.NewTicker(w.Poll)
	defer tick.Stop()
	purge := time.NewTicker(time.Hour)
	defer purge.Stop()
	for {
		for { // 有积压就连续处理
			evs, err := w.St.ClaimEvents(ctx, 50, 60*time.Second)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("webhook claim: %v", err)
				}
				break
			}
			if len(evs) == 0 {
				break
			}
			for _, e := range evs {
				w.deliver(ctx, e)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-purge.C:
			_ = w.St.PurgeEvents(ctx, 7*24*time.Hour)
		case <-tick.C:
		}
	}
}

func (w *Worker) deliver(ctx context.Context, e store.WebhookEvent) {
	wh, err := w.St.GetWebhook(ctx, e.TenantID)
	if err != nil || wh.URL == "" {
		_ = w.St.FinishEvent(ctx, e.ID, "dropped", "webhook disabled")
		return
	}
	body, _ := json.Marshal(map[string]any{
		"event_id": e.ID, "type": e.Type, "created_at": e.CreatedAt.UnixMilli(), "data": json.RawMessage(e.Payload),
	})
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, wh.URL, bytes.NewReader(body))
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "npckefu-webhook/1")
		req.Header.Set("X-Kefu-Event-Id", strconv.FormatInt(e.ID, 10))
		req.Header.Set("X-Kefu-Event-Type", e.Type)
		req.Header.Set("X-Kefu-Timestamp", ts)
		req.Header.Set("X-Kefu-Signature", Sign(wh.Secret, ts, body))
		var resp *http.Response
		if resp, err = w.client.Do(req); err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
			resp.Body.Close()
			if resp.StatusCode/100 == 2 {
				_ = w.St.FinishEvent(ctx, e.ID, "done", "")
				return
			}
			err = fmt.Errorf("status %d", resp.StatusCode)
		}
	}
	if ctx.Err() != nil {
		return // 关停中：保持 pending，租约到期后重新领取
	}
	if e.Attempts >= MaxAttempts {
		log.Printf("webhook event %d dead after %d attempts: %v", e.ID, e.Attempts, err)
		_ = w.St.FinishEvent(ctx, e.ID, "dead", err.Error())
		return
	}
	i := e.Attempts - 1
	if i >= len(w.Backoff) {
		i = len(w.Backoff) - 1
	}
	_ = w.St.RescheduleEvent(ctx, e.ID, w.Backoff[i], err.Error())
}
