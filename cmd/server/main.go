package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"npckefu/internal/api"
	"npckefu/internal/bot"
	"npckefu/internal/cluster"
	"npckefu/internal/files"
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
	// 多实例：设置 REDIS_ADDR 即启用（在线状态/跨节点投递/分布式锁走 Redis）；不设置则为单实例模式。
	var cl cluster.Cluster
	ready := []func(context.Context) error{st.Ping}
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		db, _ := strconv.Atoi(os.Getenv("REDIS_DB"))
		rc, err := cluster.NewRedis(&redis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD"), DB: db}, os.Getenv("NODE_ID"))
		if err != nil {
			log.Fatal(err)
		}
		defer rc.Close()
		cl, ready = rc, append(ready, rc.Ping)
		log.Printf("cluster mode: node=%s redis=%s", rc.NodeID(), addr)
	} else {
		log.Println("single-instance mode (set REDIS_ADDR to run multiple instances)")
	}
	svc := im.NewService(st, bot.New(st, llm), cl)
	// 图片存储：STORAGE=db（默认，存 MySQL，多实例开箱即用）| fs（UPLOAD_DIR，多实例需共享卷）| off
	switch env("STORAGE", "db") {
	case "db":
		svc.Files = files.DB{St: st}
	case "fs":
		dir := env("UPLOAD_DIR", "uploads")
		if err := os.MkdirAll(dir, 0o750); err != nil {
			log.Fatal(err)
		}
		svc.Files = files.FS{Dir: dir}
		if cl != nil {
			log.Printf("warning: STORAGE=fs with multiple instances requires %s to be a shared volume", dir)
		}
	case "off":
	default:
		log.Fatal("STORAGE must be db, fs or off")
	}
	allowPrivate := os.Getenv("WEBHOOK_ALLOW_PRIVATE") == "1" // 仅本地开发
	a := &api.API{St: st, IM: svc, AdminToken: os.Getenv("ADMIN_TOKEN"), AllowPrivateWebhook: allowPrivate, Ready: ready}
	runCtx, stopRun := context.WithCancel(context.Background())
	defer stopRun()
	go svc.Run(runCtx)
	go webhook.New(st, allowPrivate).Run(runCtx)
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
	// 优雅停机：先停止接收新连接，再主动断开 WebSocket（客户端会自动重连到其他节点），最后停后台任务。
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	svc.Shutdown()
	time.Sleep(500 * time.Millisecond) // 留时间让各连接完成注销
	stopRun()
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
