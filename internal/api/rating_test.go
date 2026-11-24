package api_test

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"npckefu/internal/im"
	"npckefu/internal/store"
)

// openHuman：新访客发起咨询并转人工，由已在线的坐席连接 ac 接待。返回访客连接与会话。
func openHuman(t *testing.T, e *env, ac *websocket.Conn) (*websocket.Conn, store.Account, *store.Session) {
	t.Helper()
	v := e.visitor(t)
	vc := dial(t, e.srv.URL, e.tenant.AppKey, v)
	t.Cleanup(func() { vc.Close() })
	vc.WriteJSON(map[string]any{"op": "start"})
	sess := wait(t, vc, func(x im.Event) bool { return x.Op == "session" }).Session
	vc.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "h", "body": "转人工"})
	got := wait(t, ac, func(x im.Event) bool {
		return x.Op == "session" && x.Session.Status == "active" && x.Session.VisitorAccid == v.Accid
	})
	return vc, v, got.Session
}

func closeBy(t *testing.T, closer, vc *websocket.Conn, sid int64) {
	t.Helper()
	closer.WriteJSON(map[string]any{"op": "close", "session_id": sid})
	wait(t, vc, func(x im.Event) bool { return x.Op == "session" && x.Session.Status == "closed" })
}

func rateWS(vc *websocket.Conn, sid int64, score int, comment string) {
	vc.WriteJSON(map[string]any{"op": "rate", "session_id": sid, "score": score, "comment": comment})
}

func wantErr(t *testing.T, c *websocket.Conn, substr string) {
	t.Helper()
	er := wait(t, c, func(x im.Event) bool { return x.Op == "error" })
	if !strings.Contains(er.Error, substr) {
		t.Fatalf("error %q does not contain %q", er.Error, substr)
	}
}

func TestSessionRating(t *testing.T) {
	e := newEnv(t)
	var agent store.Account
	do(t, "POST", e.srv.URL+"/v1/accounts", e.h, store.Account{Accid: "rate-ag", Name: "评", Role: "agent", MaxLoad: 20}, &agent)
	ac := dial(t, e.srv.URL, e.tenant.AppKey, agent)
	defer ac.Close()
	time.Sleep(200 * time.Millisecond)
	base := e.srv.URL
	getRating := func(sid int64) (store.Rating, int) {
		var r store.Rating
		c := do(t, "GET", fmt.Sprintf("%s/v1/sessions/%d/rating", base, sid), e.h, nil, &r)
		return r, c
	}

	// ---- A：正常评价 5 分 ----
	vcA, vA, sA := openHuman(t, e, ac)
	rateWS(vcA, sA.ID, 5, "很棒") // 会话还在进行中
	wantErr(t, vcA, "结束后")
	closeBy(t, ac, vcA, sA.ID)
	for _, bad := range []struct {
		score   int
		comment string
	}{{0, ""}, {6, ""}, {-1, ""}, {5, strings.Repeat("字", 501)}} {
		rateWS(vcA, sA.ID, bad.score, bad.comment)
		wait(t, vcA, func(x im.Event) bool { return x.Op == "error" })
	}
	rateWS(vcA, sA.ID, 5, "  很棒  ")
	if ev := wait(t, vcA, func(x im.Event) bool { return x.Op == "rated" }); ev.Score != 5 || ev.SessionID != sA.ID {
		t.Fatalf("rated ack: %+v", ev)
	}
	// 坐席收到评价推送
	if ev := wait(t, ac, func(x im.Event) bool { return x.Op == "rated" }); ev.Score != 5 || ev.Comment != "很棒" || ev.From != vA.Accid || ev.SessionID != sA.ID {
		t.Fatalf("agent push: %+v", ev)
	}
	// 重复评价被拒，且不覆盖
	rateWS(vcA, sA.ID, 1, "改成差评")
	wantErr(t, vcA, "已评价")
	if r, c := getRating(sA.ID); c != 200 || r.Score != 5 || r.Comment != "很棒" || r.AgentAccid != "rate-ag" || r.VisitorAccid != vA.Accid {
		t.Fatalf("stored rating: %d %+v", c, r)
	}
	// 别的访客不能评价这个会话
	vOther := e.visitor(t)
	oc := dial(t, e.srv.URL, e.tenant.AppKey, vOther)
	defer oc.Close()
	rateWS(oc, sA.ID, 1, "")
	wantErr(t, oc, "forbidden")
	// 坐席不能给自己的会话打分
	ac.WriteJSON(map[string]any{"op": "rate", "session_id": sA.ID, "score": 5})
	wantErr(t, ac, "forbidden")
	// 回调事件
	if evs := e.eventsOf("session.rated", 1); len(evs) != 1 {
		t.Fatalf("session.rated events: %d", len(evs))
	}

	// ---- B：4 分；C：2 分；E：1 分且评语命中"屏蔽"词；F：评语命中"拒绝"词 ----
	do(t, "POST", base+"/v1/sensitive-words", e.h, map[string]any{"words": []string{"烂透"}}, nil)
	do(t, "POST", base+"/v1/sensitive-words", e.h, map[string]any{"words": []string{"辱骂词"}, "action": "block"}, nil)
	for _, c := range []struct {
		score   int
		comment string
		stored  string
	}{{4, "不错", "不错"}, {2, "", ""}, {1, "这服务烂透了", "这服务**了"}} {
		vc, _, s := openHuman(t, e, ac)
		closeBy(t, ac, vc, s.ID)
		rateWS(vc, s.ID, c.score, c.comment)
		wait(t, vc, func(x im.Event) bool { return x.Op == "rated" })
		if r, _ := getRating(s.ID); r.Comment != c.stored {
			t.Fatalf("comment %q stored as %q, want %q", c.comment, r.Comment, c.stored)
		}
	}
	vcF, _, sF := openHuman(t, e, ac)
	closeBy(t, ac, vcF, sF.ID)
	rateWS(vcF, sF.ID, 3, "辱骂词")
	wantErr(t, vcF, "违规")
	if _, c := getRating(sF.ID); c != 404 {
		t.Fatalf("blocked comment must not create a rating, got %d", c)
	}

	// ---- 仅机器人接待的会话（访客自己结束），评 3 分 ----
	vB := e.visitor(t)
	vcB := dial(t, e.srv.URL, e.tenant.AppKey, vB)
	defer vcB.Close()
	vcB.WriteJSON(map[string]any{"op": "start"})
	sB := wait(t, vcB, func(x im.Event) bool { return x.Op == "session" }).Session
	closeBy(t, vcB, vcB, sB.ID)
	rateWS(vcB, sB.ID, 3, "")
	wait(t, vcB, func(x im.Event) bool { return x.Op == "rated" })
	if r, _ := getRating(sB.ID); r.AgentAccid != "" {
		t.Fatalf("bot-only rating should have empty agent: %+v", r)
	}

	// ---- 租户服务端：代评、冲突、参数校验 ----
	_, _, sG := openHuman(t, e, ac) // 进行中
	if c := do(t, "POST", fmt.Sprintf("%s/v1/sessions/%d/rating", base, sG.ID), e.h, map[string]any{"score": 5}, nil); c != 409 {
		t.Fatalf("rate open session via REST: want 409, got %d", c)
	}
	vcH, _, sH := openHuman(t, e, ac)
	closeBy(t, ac, vcH, sH.ID)
	rurl := fmt.Sprintf("%s/v1/sessions/%d/rating", base, sH.ID)
	if c := do(t, "POST", rurl, e.h, map[string]any{"score": 9}, nil); c != 400 {
		t.Fatalf("bad score: want 400, got %d", c)
	}
	if c := do(t, "POST", rurl, e.h, map[string]any{"score": 4, "comment": "代评"}, nil); c != 201 {
		t.Fatalf("on-behalf: want 201, got %d", c)
	}
	if c := do(t, "POST", rurl, e.h, map[string]any{"score": 4}, nil); c != 409 {
		t.Fatalf("duplicate: want 409, got %d", c)
	}
	if c := do(t, "POST", base+"/v1/sessions/99999999/rating", e.h, map[string]any{"score": 4}, nil); c != 404 {
		t.Fatalf("unknown session: want 404, got %d", c)
	}

	// ---- 超过 7 天不能评 ----
	vcI, _, sI := openHuman(t, e, ac)
	closeBy(t, ac, vcI, sI.ID)
	raw, err := sql.Open("mysql", os.Getenv("TEST_MYSQL_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Exec(`UPDATE sessions SET closed_at=DATE_SUB(NOW(), INTERVAL 8 DAY) WHERE id=?`, sI.ID); err != nil {
		t.Fatal(err)
	}
	rateWS(vcI, sI.ID, 5, "")
	wantErr(t, vcI, "有效期")

	// ---- 统计：A=5 B=4 C=2 E=1（坐席 rate-ag），Bot=3，H=4（坐席 rate-ag，代评）----
	var st store.RatingStats
	do(t, "GET", base+"/v1/stats/ratings", e.h, nil, &st)
	// 分数：5,4,2,1,3(bot),4 → 共 6，总分 19，均分 3.17，4~5 分 3 个 → 0.5
	if st.Count != 6 || st.Avg != 3.17 || st.SatisfactionRate != 0.5 ||
		st.Distribution["1"] != 1 || st.Distribution["2"] != 1 || st.Distribution["3"] != 1 || st.Distribution["4"] != 2 || st.Distribution["5"] != 1 {
		t.Fatalf("stats: %+v", st)
	}
	if len(st.ByAgent) != 2 || st.ByAgent[0].AgentAccid != "rate-ag" || st.ByAgent[0].Count != 5 || st.ByAgent[0].Avg != 3.2 || st.ByAgent[1].AgentAccid != "" || st.ByAgent[1].Count != 1 {
		t.Fatalf("by_agent: %+v", st.ByAgent)
	}
	var stA store.RatingStats
	do(t, "GET", base+"/v1/stats/ratings?agent=rate-ag&from=2000-01-01&to="+time.Now().UTC().Format("2006-01-02"), e.h, nil, &stA)
	if stA.Count != 5 || len(stA.ByAgent) != 1 {
		t.Fatalf("agent stats (date-only 'to' must include today): %+v", stA)
	}
	var future store.RatingStats
	do(t, "GET", base+"/v1/stats/ratings?from="+time.Now().UTC().Add(48*time.Hour).Format("2006-01-02"), e.h, nil, &future)
	if future.Count != 0 || future.Avg != 0 {
		t.Fatalf("future: %+v", future)
	}
	if c := do(t, "GET", base+"/v1/stats/ratings?from=yesterday", e.h, nil, nil); c != 400 {
		t.Fatalf("bad date: want 400, got %d", c)
	}

	// ---- 列表与过滤 ----
	var all, low, mine []store.Rating
	do(t, "GET", base+"/v1/ratings", e.h, nil, &all)
	do(t, "GET", base+"/v1/ratings?max_score=2", e.h, nil, &low)
	do(t, "GET", base+"/v1/ratings?agent=rate-ag&limit=2", e.h, nil, &mine)
	if len(all) != 6 || len(low) != 2 || len(mine) != 2 {
		t.Fatalf("lists: all=%d low=%d mine=%d", len(all), len(low), len(mine))
	}
	if all[0].SessionID < all[len(all)-1].SessionID {
		t.Fatal("list should be newest first")
	}
	var page2 []store.Rating
	do(t, "GET", fmt.Sprintf("%s/v1/ratings?agent=rate-ag&limit=2&before=%d", base, mine[1].SessionID), e.h, nil, &page2)
	if len(page2) != 2 || page2[0].SessionID >= mine[1].SessionID {
		t.Fatalf("pagination: %+v", page2)
	}
}
