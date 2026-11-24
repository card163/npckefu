package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"npckefu/internal/api"
	"npckefu/internal/bot"
	"npckefu/internal/im"
	"npckefu/internal/store"
	"npckefu/internal/webhook"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	st, err := store.Open(env("DB_DSN", "root:@tcp(127.0.0.1:3306)/npckefu"))
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()

	var llm *bot.LLM
	if key := os.Getenv("LLM_API_KEY"); key != "" {
		llm = &bot.LLM{BaseURL: env("LLM_BASE_URL", "https://api.openai.com/v1"), APIKey: key, Model: env("LLM_MODEL", "gpt-4o-mini")}
		log.Println("LLM enabled:", llm.Model)
	}
	svc := im.NewService(st, bot.New(st, llm))
	allowPrivate := os.Getenv("WEBHOOK_ALLOW_PRIVATE") == "1" // 仅本地开发
	a := &api.API{St: st, IM: svc, AdminToken: os.Getenv("ADMIN_TOKEN"), AllowPrivateWebhook: allowPrivate}
	wctx, stopWebhook := context.WithCancel(context.Background())
	defer stopWebhook()
	go webhook.New(st, allowPrivate).Run(wctx)
	if a.AdminToken == "" {
		log.Println("warning: ADMIN_TOKEN not set; tenant creation disabled")
	}

	if os.Getenv("SEED_DEMO") == "1" {
		seedDemo(st)
	}
	srv := &http.Server{Addr: env("ADDR", ":8080"), Handler: a.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Println("listening on", srv.Addr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

// seedDemo 创建演示租户/坐席/FAQ，并打印登录信息（仅本地体验用）。
func seedDemo(st *store.Store) {
	ctx := context.Background()
	t, err := st.CreateTenant(ctx, "demo")
	if err != nil {
		log.Fatal(err)
	}
	a, err := st.UpsertAccount(ctx, &store.Account{TenantID: t.ID, Accid: "agent1", Name: "小王", Role: store.RoleAgent})
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range []store.FAQ{
		{Question: "如何申请退款", Answer: "请在“我的订单”中点击“申请退款”，审核通过后 1-3 个工作日原路退回。", Keywords: "退款,退钱"},
		{Question: "发货时间是多久", Answer: "下单后 48 小时内发货，偏远地区可能延迟。", Keywords: "发货,物流,快递"},
	} {
		f.TenantID = t.ID
		_ = st.AddFAQ(ctx, &f)
	}
	log.Printf("DEMO visitor: /visitor.html?appkey=%s", t.AppKey)
	log.Printf("DEMO agent:   /agent.html  appkey=%s accid=%s token=%s", t.AppKey, a.Accid, a.Token)
}
