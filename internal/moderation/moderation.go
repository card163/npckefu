// Package moderation 提供按租户隔离的敏感词过滤：Aho-Corasick 多模式匹配，命中后"屏蔽"或"拒绝"。
//
// 匹配前会做归一化，使常见的规避写法也能命中：大小写、全角半角、以及词中夹杂的空格/标点/零宽字符
// （如 "傻 逼"、"傻.逼"）。注意：这不覆盖谐音、拼音、形近字、拆字等变体，需要另行扩充词库或接入专业内容安全服务。
package moderation

import (
	"context"
	"log"
	"sync"
	"time"
	"unicode"

	"npckefu/internal/store"
)

const (
	ActionMask  = "mask"  // 命中部分替换为 *
	ActionBlock = "block" // 整条消息拒绝发送
)

// MinWordRunes 过短的词误伤太大，不允许配置。
const MinWordRunes = 2

type Result struct {
	Hit   bool
	Block bool
	Text  string   // 屏蔽后的文本（Block 时为原文）
	Words []string // 命中的词（去重）
}

// norm 返回归一化后的字符；keep=false 表示该字符在匹配时被忽略（空格、标点、零宽字符等）。
func norm(r rune) (rune, bool) {
	switch {
	case r >= 0xFF01 && r <= 0xFF5E: // 全角 ASCII → 半角
		r -= 0xFEE0
	case r == 0x3000:
		return 0, false
	}
	if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
		return 0, false
	}
	return unicode.ToLower(r), true
}

func normalize(s string) []rune {
	var out []rune
	for _, r := range s {
		if n, ok := norm(r); ok {
			out = append(out, n)
		}
	}
	return out
}

// ValidWord 返回归一化后的长度是否达标。
func ValidWord(w string) bool { return len(normalize(w)) >= MinWordRunes }

type node struct {
	next map[rune]int32
	fail int32
	out  []int32 // 以该节点结尾的词下标（含沿 fail 链继承的）
}

type Matcher struct {
	nodes []node
	words []store.SensitiveWord
	lens  []int // 各词归一化长度
}

func NewMatcher(words []store.SensitiveWord) *Matcher {
	m := &Matcher{nodes: []node{{next: map[rune]int32{}}}}
	for _, w := range words {
		rs := normalize(w.Word)
		if len(rs) < MinWordRunes {
			continue
		}
		cur := int32(0)
		for _, r := range rs {
			nx, ok := m.nodes[cur].next[r]
			if !ok {
				nx = int32(len(m.nodes))
				m.nodes = append(m.nodes, node{next: map[rune]int32{}})
				m.nodes[cur].next[r] = nx
			}
			cur = nx
		}
		m.words = append(m.words, w)
		m.lens = append(m.lens, len(rs))
		m.nodes[cur].out = append(m.nodes[cur].out, int32(len(m.words)-1))
	}
	// BFS 建 fail 链
	queue := []int32{}
	for _, c := range m.nodes[0].next {
		queue = append(queue, c)
	}
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		for r, v := range m.nodes[u].next {
			f := m.nodes[u].fail
			for f != 0 {
				if _, ok := m.nodes[f].next[r]; ok {
					break
				}
				f = m.nodes[f].fail
			}
			if t, ok := m.nodes[f].next[r]; ok && t != v {
				m.nodes[v].fail = t
			}
			m.nodes[v].out = append(m.nodes[v].out, m.nodes[m.nodes[v].fail].out...)
			queue = append(queue, v)
		}
	}
	return m
}

func (m *Matcher) Empty() bool { return m == nil || len(m.words) == 0 }

// Check 扫描文本。
func (m *Matcher) Check(text string) Result {
	res := Result{Text: text}
	if m.Empty() {
		return res
	}
	orig := []rune(text)
	var rs []rune
	var pos []int // 归一化字符 → 原文下标
	for i, r := range orig {
		if n, ok := norm(r); ok {
			rs = append(rs, n)
			pos = append(pos, i)
		}
	}
	mask := make([]bool, len(orig))
	seen := map[int32]bool{}
	cur := int32(0)
	for i, r := range rs {
		for cur != 0 {
			if _, ok := m.nodes[cur].next[r]; ok {
				break
			}
			cur = m.nodes[cur].fail
		}
		if nx, ok := m.nodes[cur].next[r]; ok {
			cur = nx
		}
		for _, wi := range m.nodes[cur].out {
			start := pos[i-m.lens[wi]+1]
			for k := start; k <= pos[i]; k++ {
				mask[k] = true
			}
			if !seen[wi] {
				seen[wi] = true
				res.Words = append(res.Words, m.words[wi].Word)
			}
			if m.words[wi].Action == ActionBlock {
				res.Block = true
			}
		}
	}
	if len(res.Words) == 0 {
		return res
	}
	res.Hit = true
	if !res.Block {
		for i := range orig {
			if mask[i] {
				orig[i] = '*'
			}
		}
		res.Text = string(orig)
	}
	return res
}

// ---- 按租户缓存 ----

type Loader interface {
	ListSensitiveWords(ctx context.Context, tenantID int64) ([]store.SensitiveWord, error)
}

type Moderator struct {
	L   Loader
	TTL time.Duration // 其他实例改词库后的最长生效延迟；本实例改动会立即生效（Invalidate）
	mu  sync.Mutex
	c   map[int64]entry
}

type entry struct {
	m  *Matcher
	at time.Time
}

func New(l Loader) *Moderator { return &Moderator{L: l, TTL: 30 * time.Second, c: map[int64]entry{}} }

func (d *Moderator) Invalidate(tenantID int64) {
	d.mu.Lock()
	delete(d.c, tenantID)
	d.mu.Unlock()
}

func (d *Moderator) matcher(ctx context.Context, tenantID int64) *Matcher {
	d.mu.Lock()
	e, ok := d.c[tenantID]
	d.mu.Unlock()
	if ok && time.Since(e.at) < d.TTL {
		return e.m
	}
	words, err := d.L.ListSensitiveWords(ctx, tenantID)
	if err != nil {
		// 词库暂时读不到时沿用旧缓存（没有则放行）：聊天可用性优先，并记录日志。
		log.Printf("moderation: load words tenant=%d: %v", tenantID, err)
		return e.m
	}
	m := NewMatcher(words)
	d.mu.Lock()
	d.c[tenantID] = entry{m, time.Now()}
	d.mu.Unlock()
	return m
}

func (d *Moderator) Check(ctx context.Context, tenantID int64, text string) Result {
	return d.matcher(ctx, tenantID).Check(text)
}
