// Package bot 实现智能应答：FAQ 知识库检索 + 可选 LLM（OpenAI 兼容接口）+ 转人工意图识别。
package bot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode"

	"npckefu/internal/store"
)

type Reply struct {
	Text       string
	Confidence float64
	Source     string // faq | llm | fallback
}

type LLM struct {
	BaseURL, APIKey, Model string
	HTTP                   *http.Client
}

type Bot struct {
	st        *store.Store
	llm       *LLM // 可为 nil
	Threshold float64
}

func New(st *store.Store, llm *LLM) *Bot {
	if llm != nil && llm.HTTP == nil {
		llm.HTTP = &http.Client{Timeout: 30 * time.Second}
	}
	return &Bot{st: st, llm: llm, Threshold: 0.35}
}

var handoffWords = []string{"转人工", "人工客服", "人工服务", "找人工", "真人", "投诉", "human", "agent"}

// WantsHuman 判断用户是否明确要求人工。
func WantsHuman(q string) bool {
	q = strings.ToLower(q)
	for _, w := range handoffWords {
		if strings.Contains(q, w) {
			return true
		}
	}
	return false
}

// Answer 先查 FAQ；命中置信度够高直接回答；否则若配置了 LLM，则以 FAQ 为上下文生成回答。
func (b *Bot) Answer(ctx context.Context, tenantID int64, q string) (Reply, error) {
	faqs, err := b.st.ListFAQs(ctx, tenantID)
	if err != nil {
		return Reply{}, err
	}
	top := rank(q, faqs)
	if len(top) > 0 && top[0].score >= b.Threshold {
		return Reply{Text: top[0].faq.Answer, Confidence: top[0].score, Source: "faq"}, nil
	}
	if b.llm != nil {
		var ctxParts []string
		for i, t := range top {
			if i >= 3 {
				break
			}
			ctxParts = append(ctxParts, fmt.Sprintf("Q: %s\nA: %s", t.faq.Question, t.faq.Answer))
		}
		txt, err := b.llm.chat(ctx, strings.Join(ctxParts, "\n\n"), q)
		if err == nil && strings.TrimSpace(txt) != "" {
			return Reply{Text: txt, Confidence: 0.5, Source: "llm"}, nil
		}
	}
	return Reply{Text: "抱歉，我暂时没能理解您的问题。您可以换个说法，或输入“转人工”联系人工客服。", Source: "fallback"}, nil
}

type scored struct {
	faq   store.FAQ
	score float64
}

func rank(q string, faqs []store.FAQ) []scored {
	qg := grams(q)
	var out []scored
	for _, f := range faqs {
		s := jaccard(qg, grams(f.Question+" "+f.Keywords))
		// 关键词精确包含加权
		for _, k := range strings.Fields(strings.NewReplacer("，", " ", ",", " ", "、", " ").Replace(f.Keywords)) {
			if k != "" && strings.Contains(q, k) {
				s += 0.3
			}
		}
		if s > 1 {
			s = 1
		}
		out = append(out, scored{f, s})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].score > out[j].score })
	return out
}

// grams 生成字符 bigram 集合（对中文无需分词即可工作）。
func grams(s string) map[string]struct{} {
	var rs []rune
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			rs = append(rs, r)
		}
	}
	m := map[string]struct{}{}
	if len(rs) == 1 {
		m[string(rs)] = struct{}{}
	}
	for i := 0; i+1 < len(rs); i++ {
		m[string(rs[i:i+2])] = struct{}{}
	}
	return m
}

func jaccard(a, b map[string]struct{}) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	inter := 0
	for k := range a {
		if _, ok := b[k]; ok {
			inter++
		}
	}
	// 以查询侧覆盖率为主，避免长 FAQ 稀释分数
	return float64(inter)/float64(len(a))*0.7 + float64(inter)/float64(len(a)+len(b)-inter)*0.3
}

func (l *LLM) chat(ctx context.Context, knowledge, q string) (string, error) {
	sys := "你是企业智能客服。仅依据给定知识库回答；知识库无法回答时，请礼貌说明并建议用户转人工。回答简洁。\n知识库：\n" + knowledge
	body, _ := json.Marshal(map[string]any{
		"model": l.Model,
		"messages": []map[string]string{
			{"role": "system", "content": sys},
			{"role": "user", "content": q},
		},
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(l.BaseURL, "/")+"/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+l.APIKey)
	resp, err := l.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("llm status %d", resp.StatusCode)
	}
	var out struct {
		Choices []struct {
			Message struct{ Content string } `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", err
	}
	if len(out.Choices) == 0 {
		return "", errors.New("llm: empty choices")
	}
	return out.Choices[0].Message.Content, nil
}
