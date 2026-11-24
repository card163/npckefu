package moderation

import (
	"testing"

	"npckefu/internal/store"
)

func TestMatcher(t *testing.T) {
	m := NewMatcher([]store.SensitiveWord{
		{Word: "傻逼", Action: ActionMask},
		{Word: "Fuck", Action: ActionMask},
		{Word: "赌博网站", Action: ActionBlock},
		{Word: "he", Action: ActionMask}, // 归一化后长度 2
		{Word: "a", Action: ActionMask},  // 过短，忽略
		{Word: "she", Action: ActionMask},
	})
	cases := []struct {
		in, out    string
		hit, block bool
	}{
		{"你好，世界", "你好，世界", false, false},
		{"你是傻逼吧", "你是**吧", true, false},
		{"你是傻 逼吧", "你是***吧", true, false},     // 夹空格：整段被屏蔽
		{"你是傻.。逼", "你是****", true, false},     // 夹标点
		{"你是傻​逼", "你是***", true, false},       // 零宽字符
		{"FUCK you", "**** you", true, false}, // 大小写
		{"ＦＵＣＫ", "****", true, false},         // 全角
		{"去赌博网站吧", "去赌博网站吧", true, true},      // block：保留原文
		{"ushers", "u***rs", true, false},     // she/he 重叠，AC 经 fail 链应全部命中
		{"a", "a", false, false},
	}
	for _, c := range cases {
		r := m.Check(c.in)
		if r.Hit != c.hit || r.Block != c.block || r.Text != c.out {
			t.Errorf("%q => hit=%v block=%v %q; want hit=%v block=%v %q", c.in, r.Hit, r.Block, r.Text, c.hit, c.block, c.out)
		}
	}
	if !ValidWord("傻逼") || ValidWord("a") || ValidWord("！！") {
		t.Error("ValidWord")
	}
	var empty *Matcher
	if r := empty.Check("x"); r.Hit {
		t.Error("nil matcher")
	}
}
