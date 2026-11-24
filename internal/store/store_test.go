package store_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"npckefu/internal/store"
)

func open(t *testing.T) *store.Store {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN to run")
	}
	st, err := store.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// 并发写同一会话：seq 必须连续无重复；相同 client_msg_id 只落一条。
func TestAppendMessageConcurrent(t *testing.T) {
	st, ctx := open(t), context.Background()
	tn, _ := st.CreateTenant(ctx, "t")
	sess, err := st.CreateSession(ctx, tn.ID, "v1", "")
	if err != nil {
		t.Fatal(err)
	}
	const n = 40
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for _, cid := range []string{fmt.Sprintf("c%d", i), fmt.Sprintf("c%d", i), ""} { // 每条重发一次 + 一条无 client_msg_id
				if _, _, err := st.AppendMessage(ctx, &store.Message{TenantID: tn.ID, SessionID: sess.ID, ClientMsgID: cid, From: "v1", FromRole: "visitor", Type: "text", Body: "你好🙂"}); err != nil {
					t.Error(err)
				}
			}
		}(i)
	}
	wg.Wait()
	msgs, err := st.MessagesSince(ctx, tn.ID, sess.ID, 0, 500)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2*n {
		t.Fatalf("want %d msgs, got %d", 2*n, len(msgs))
	}
	for i, m := range msgs {
		if m.Seq != int64(i+1) {
			t.Fatalf("seq gap at %d: %d", i, m.Seq)
		}
		if m.Body != "你好🙂" {
			t.Fatalf("utf8mb4 roundtrip failed: %q", m.Body)
		}
	}
}
