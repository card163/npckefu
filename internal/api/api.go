// Package api 提供租户服务端 REST 接口（账号注册、知识库、会话/消息查询）与 WebSocket 入口。
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"npckefu/internal/files"
	"npckefu/internal/im"
	"npckefu/internal/moderation"
	"npckefu/internal/store"
	"npckefu/internal/webhook"
	"npckefu/web"
)

type API struct {
	St         *store.Store
	IM         *im.Service
	AdminToken string // 平台超管令牌，用于创建租户
	// AllowPrivateWebhook 仅开发/测试：允许回调地址指向内网/回环。
	AllowPrivateWebhook bool
	// Ready 就绪检查（数据库、Redis 等），供 /readyz 使用。
	Ready []func(context.Context) error
}

var accidRe = regexp.MustCompile(`^[A-Za-z0-9_.@-]{1,64}$`)

type ctxKey struct{}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		for _, f := range a.Ready {
			if err := f(ctx); err != nil {
				log.Printf("readyz: %v", err)
				http.Error(w, "not ready", http.StatusServiceUnavailable)
				return
			}
		}
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /ws", a.IM.ServeWS)

	// 平台级
	mux.HandleFunc("POST /v1/tenants", a.createTenant)

	// 访客自助注册（只需 appkey，客户端可直接调用；生产请加限流/人机校验）
	mux.HandleFunc("POST /v1/visitors/register", a.registerVisitor)

	// 图片：上传（账号凭 X-App-Key+X-Accid+X-Token，或租户服务端凭 X-App-Key+X-App-Secret）与读取（持有链接）
	mux.HandleFunc("POST /v1/upload", a.upload)
	mux.HandleFunc("GET /files/{id}", a.serveFile)

	// 租户服务端接口（X-App-Key + X-App-Secret）
	mux.HandleFunc("POST /v1/accounts", a.tenantAuth(a.upsertAccount))
	mux.HandleFunc("GET /v1/accounts", a.tenantAuth(a.listAccounts))
	mux.HandleFunc("POST /v1/faqs", a.tenantAuth(a.addFAQ))
	mux.HandleFunc("GET /v1/faqs", a.tenantAuth(a.listFAQs))
	mux.HandleFunc("DELETE /v1/faqs/{id}", a.tenantAuth(a.deleteFAQ))
	mux.HandleFunc("PUT /v1/webhook", a.tenantAuth(a.putWebhook))
	mux.HandleFunc("GET /v1/webhook", a.tenantAuth(a.getWebhook))
	mux.HandleFunc("DELETE /v1/webhook", a.tenantAuth(a.deleteWebhook))
	mux.HandleFunc("POST /v1/messages", a.tenantAuth(a.sendMessage))
	mux.HandleFunc("GET /v1/accounts/{accid}/recent", a.tenantAuth(a.recent))
	mux.HandleFunc("GET /v1/accounts/{accid}/presence", a.tenantAuth(a.presence))
	mux.HandleFunc("POST /v1/presence", a.tenantAuth(a.presenceBatch))
	mux.HandleFunc("PUT /v1/accounts/{accid}/devices", a.tenantAuth(a.putDevice))
	mux.HandleFunc("DELETE /v1/accounts/{accid}/devices/{token}", a.tenantAuth(a.deleteDevice))
	mux.HandleFunc("POST /v1/sensitive-words", a.tenantAuth(a.addWords))
	mux.HandleFunc("GET /v1/sensitive-words", a.tenantAuth(a.listWords))
	mux.HandleFunc("DELETE /v1/sensitive-words/{id}", a.tenantAuth(a.deleteWord))
	mux.HandleFunc("GET /v1/tenant", a.tenantAuth(a.tenantInfo))
	mux.HandleFunc("GET /v1/sessions/{id}", a.tenantAuth(a.getSession))
	mux.HandleFunc("GET /v1/stats/overview", a.tenantAuth(a.overview))
	mux.HandleFunc("GET /v1/agents/roster", a.tenantAuth(a.roster))
	mux.HandleFunc("POST /v1/sessions/{id}/close", a.tenantAuth(a.closeSession))
	mux.HandleFunc("PUT /v1/faqs/{id}", a.tenantAuth(a.updateFAQ))
	mux.HandleFunc("POST /v1/accounts/{accid}/token", a.tenantAuth(a.resetToken))
	mux.HandleFunc("GET /v1/webhook/events", a.tenantAuth(a.webhookEvents))
	mux.HandleFunc("POST /v1/webhook/events/{id}/retry", a.tenantAuth(a.retryWebhookEvent))
	mux.HandleFunc("POST /v1/sessions/{id}/transfer", a.tenantAuth(a.transferSession))
	mux.HandleFunc("GET /v1/sessions/{id}/transfers", a.tenantAuth(a.listTransfers))
	mux.HandleFunc("POST /v1/sessions/{id}/rating", a.tenantAuth(a.rateSession))
	mux.HandleFunc("GET /v1/sessions/{id}/rating", a.tenantAuth(a.getRating))
	mux.HandleFunc("GET /v1/ratings", a.tenantAuth(a.listRatings))
	mux.HandleFunc("GET /v1/stats/ratings", a.tenantAuth(a.ratingStats))
	mux.HandleFunc("GET /v1/sessions", a.tenantAuth(a.listSessions))
	mux.HandleFunc("GET /v1/sessions/{id}/messages", a.tenantAuth(a.listMessages))
	mux.Handle("GET /", http.FileServerFS(web.FS)) // /visitor.html /agent.html
	return logging(mux)
}

func (a *API) tenantAuth(h func(http.ResponseWriter, *http.Request, *store.Tenant)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		t, err := a.St.TenantByAppKey(r.Context(), r.Header.Get("X-App-Key"))
		if err != nil || subtle.ConstantTimeCompare([]byte(t.AppSecret), []byte(r.Header.Get("X-App-Secret"))) != 1 {
			writeErr(w, http.StatusUnauthorized, "invalid app credentials")
			return
		}
		h(w, r, t)
	}
}

func (a *API) createTenant(w http.ResponseWriter, r *http.Request) {
	if a.AdminToken == "" || subtle.ConstantTimeCompare([]byte(a.AdminToken), []byte(r.Header.Get("X-Admin-Token"))) != 1 {
		writeErr(w, http.StatusUnauthorized, "admin token required")
		return
	}
	var in struct{ Name string }
	if !decode(w, r, &in) || in.Name == "" {
		writeErr(w, http.StatusBadRequest, "name required")
		return
	}
	t, err := a.St.CreateTenant(r.Context(), in.Name)
	if err != nil {
		serverErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, t)
}

func (a *API) registerVisitor(w http.ResponseWriter, r *http.Request) {
	var in struct {
		AppKey string `json:"app_key"`
		Name   string `json:"name"`
	}
	if !decode(w, r, &in) {
		return
	}
	t, err := a.St.TenantByAppKey(r.Context(), in.AppKey)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, "invalid app_key")
		return
	}
	acc, err := a.St.UpsertAccount(r.Context(), &store.Account{TenantID: t.ID, Accid: "v_" + store.RandHex(8), Name: in.Name, Role: store.RoleVisitor})
	if err != nil {
		serverErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, acc)
}

func (a *API) upsertAccount(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	var in store.Account
	if !decode(w, r, &in) {
		return
	}
	if in.Role != store.RoleAgent && in.Role != store.RoleVisitor {
		writeErr(w, http.StatusBadRequest, "role must be agent|visitor")
		return
	}
	if !accidRe.MatchString(in.Accid) {
		writeErr(w, http.StatusBadRequest, "accid: 1-64 chars of letters, digits, _ . @ -")
		return
	}
	if len(in.Name) > 128 || len(in.SkillGroup) > 64 || in.MaxLoad < 0 || in.MaxLoad > 1000 {
		writeErr(w, http.StatusBadRequest, "name<=128, skill_group<=64, max_load 0-1000 (0 = default 5)")
		return
	}
	// 已存在账号不允许借 upsert 改角色，避免提权
	if ex, err := a.St.Account(r.Context(), t.ID, in.Accid); err == nil && ex.Role != in.Role {
		writeErr(w, http.StatusConflict, "role cannot be changed")
		return
	}
	in.TenantID, in.Token = t.ID, ""
	acc, err := a.St.UpsertAccount(r.Context(), &in)
	if err != nil {
		serverErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, acc)
}

func (a *API) listAccounts(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	v, err := a.St.ListAccounts(r.Context(), t.ID, r.URL.Query().Get("role"))
	if err != nil {
		serverErr(w, err)
		return
	}
	type view struct { // 附带在线状态，方便坐席管理页展示
		store.Account
		Online bool `json:"online"`
	}
	out := make([]view, 0, len(v))
	for _, x := range v {
		out = append(out, view{x, a.IM.Hub.Online(t.ID, x.Accid)})
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *API) addFAQ(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	var f store.FAQ
	if !decode(w, r, &f) || f.Question == "" || f.Answer == "" {
		writeErr(w, http.StatusBadRequest, "question and answer required")
		return
	}
	f.TenantID = t.ID
	if err := a.St.AddFAQ(r.Context(), &f); err != nil {
		serverErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, f)
}

func (a *API) listFAQs(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	v, err := a.St.ListFAQs(r.Context(), t.ID)
	respond(w, v, err)
}

func (a *API) deleteFAQ(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err := a.St.DeleteFAQ(r.Context(), t.ID, id); err != nil {
		serverErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) listSessions(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	before, _ := strconv.ParseInt(q.Get("before"), 10, 64)
	v, err := a.St.FindSessions(r.Context(), t.ID, store.SessionFilter{Status: q.Get("status"), Agent: q.Get("agent"), Visitor: q.Get("visitor"), BeforeID: before, Limit: limit})
	respond(w, v, err)
}

func (a *API) listMessages(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
	v, err := a.St.MessagesSince(r.Context(), t.ID, id, since, 500)
	respond(w, v, err)
}

func (a *API) putWebhook(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	var in struct {
		URL          string `json:"url"`
		RotateSecret bool   `json:"rotate_secret"`
	}
	if !decode(w, r, &in) {
		return
	}
	if err := webhook.ValidateURL(in.URL, a.AllowPrivateWebhook); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	cur, err := a.St.GetWebhook(r.Context(), t.ID)
	if err != nil {
		serverErr(w, err)
		return
	}
	if cur.Secret == "" || in.RotateSecret {
		cur.Secret = store.RandHex(24)
	}
	if err := a.St.SetWebhook(r.Context(), t.ID, in.URL, cur.Secret); err != nil {
		serverErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, store.Webhook{URL: in.URL, Secret: cur.Secret})
}

func (a *API) getWebhook(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	v, err := a.St.GetWebhook(r.Context(), t.ID)
	respondObj(w, v, err)
}

func (a *API) deleteWebhook(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	if err := a.St.SetWebhook(r.Context(), t.ID, "", ""); err != nil {
		serverErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// sendMessage 租户服务端向会话或访客下发消息。
func (a *API) sendMessage(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	var in struct {
		SessionID int64  `json:"session_id"`
		To        string `json:"to"` // 访客 accid
		im.SendReq
	}
	if !decode(w, r, &in) {
		return
	}
	m, err := a.IM.SendNotice(r.Context(), t.ID, in.SessionID, in.To, in.SendReq)
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, m)
	case errors.Is(err, im.ErrBadFile):
		writeErr(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, im.ErrBadInput):
		writeErr(w, http.StatusBadRequest, "need body (<=8192B, type text|image|custom) and session_id or visitor accid in 'to'")
	case errors.Is(err, im.ErrClosed):
		writeErr(w, http.StatusConflict, "session closed")
	default:
		serverErr(w, err)
	}
}

func (a *API) recent(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	accid := r.PathValue("accid")
	if _, err := a.St.Account(r.Context(), t.ID, accid); err != nil {
		serverErr(w, err)
		return
	}
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	v, err := a.St.RecentSessions(r.Context(), t.ID, accid, q.Get("include_closed") == "1", limit)
	if err == nil && v == nil {
		v = []store.RecentItem{}
	}
	respondObj(w, v, err)
}

func (a *API) putDevice(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	var in struct{ Platform, Token string }
	if !decode(w, r, &in) {
		return
	}
	acc, err := a.St.Account(r.Context(), t.ID, r.PathValue("accid"))
	if err != nil {
		serverErr(w, err)
		return
	}
	if err := a.IM.RegisterDevice(r.Context(), acc, in.Platform, in.Token); err != nil {
		if errors.Is(err, im.ErrBadInput) {
			writeErr(w, http.StatusBadRequest, "platform (<=16) and token (<=512) required")
			return
		}
		serverErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) deleteDevice(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	if err := a.St.DeleteDevice(r.Context(), t.ID, r.PathValue("accid"), r.PathValue("token")); err != nil {
		serverErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// addWords 批量添加敏感词：{"words":["…"],"action":"mask|block"}（默认 mask）。已存在的词会更新 action。
func (a *API) addWords(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	var in struct {
		Words  []string `json:"words"`
		Action string   `json:"action"`
	}
	if !decode(w, r, &in) {
		return
	}
	if in.Action == "" {
		in.Action = moderation.ActionMask
	}
	if (in.Action != moderation.ActionMask && in.Action != moderation.ActionBlock) || len(in.Words) == 0 || len(in.Words) > 1000 {
		writeErr(w, http.StatusBadRequest, "words (1-1000) required; action must be mask|block")
		return
	}
	for _, x := range in.Words {
		if len(x) > 128 || !moderation.ValidWord(x) {
			writeErr(w, http.StatusBadRequest, "each word needs at least 2 letters/digits/characters (and <=128 bytes): "+x)
			return
		}
	}
	n, err := a.St.CountSensitiveWords(r.Context(), t.ID)
	if err != nil {
		serverErr(w, err)
		return
	}
	if n+len(in.Words) > store.MaxSensitiveWords {
		writeErr(w, http.StatusConflict, "sensitive word limit reached")
		return
	}
	affected, err := a.St.AddSensitiveWords(r.Context(), t.ID, in.Words, in.Action)
	a.IM.Mod.Invalidate(t.ID)
	if err != nil {
		serverErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"affected": affected})
}

func (a *API) listWords(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	v, err := a.St.ListSensitiveWords(r.Context(), t.ID)
	if err == nil && v == nil {
		v = []store.SensitiveWord{}
	}
	respondObj(w, v, err)
}

func (a *API) deleteWord(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	err := a.St.DeleteSensitiveWord(r.Context(), t.ID, id)
	a.IM.Mod.Invalidate(t.ID)
	if err != nil {
		serverErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) presence(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	ps, err := a.IM.Presences(r.Context(), t.ID, []string{r.PathValue("accid")})
	if err != nil {
		serverErr(w, err)
		return
	}
	if !ps[0].Exists {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	writeJSON(w, http.StatusOK, ps[0])
}

// presenceBatch 批量查询（最多 100 个）：{"accids":["a","b"]}。
func (a *API) presenceBatch(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	var in struct {
		Accids []string `json:"accids"`
	}
	if !decode(w, r, &in) {
		return
	}
	if len(in.Accids) == 0 || len(in.Accids) > 100 {
		writeErr(w, http.StatusBadRequest, "accids: 1-100 items")
		return
	}
	ps, err := a.IM.Presences(r.Context(), t.ID, in.Accids)
	respondObj(w, ps, err)
}

const (
	uploadsPerMinute = 30
)

// uploadIdentity 识别上传者：租户服务端（返回 business）或某个账号（返回其 accid）。
func (a *API) uploadIdentity(r *http.Request) (tenantID int64, uploader string, ok bool) {
	t, err := a.St.TenantByAppKey(r.Context(), r.Header.Get("X-App-Key"))
	if err != nil {
		return 0, "", false
	}
	if sec := r.Header.Get("X-App-Secret"); sec != "" {
		if subtle.ConstantTimeCompare([]byte(t.AppSecret), []byte(sec)) != 1 {
			return 0, "", false
		}
		return t.ID, im.BusinessSender, true
	}
	acc, err := a.St.Account(r.Context(), t.ID, r.Header.Get("X-Accid"))
	if err != nil || subtle.ConstantTimeCompare([]byte(acc.Token), []byte(r.Header.Get("X-Token"))) != 1 {
		return 0, "", false
	}
	return t.ID, acc.Accid, true
}

// upload：multipart/form-data，字段名 file。校验内容真实类型/尺寸，存储后返回 file_id，
// 随后用 {"op":"send","type":"image","body":"{\"file_id\":\"…\"}"} 发送。
func (a *API) upload(w http.ResponseWriter, r *http.Request) {
	if a.IM.Files == nil {
		writeErr(w, http.StatusNotFound, "image upload is not enabled")
		return
	}
	tenantID, uploader, ok := a.uploadIdentity(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "invalid credentials")
		return
	}
	if n, err := a.St.CountRecentUploads(r.Context(), tenantID, uploader, time.Minute); err != nil {
		serverErr(w, err)
		return
	} else if n >= uploadsPerMinute {
		w.Header().Set("Retry-After", "60")
		writeErr(w, http.StatusTooManyRequests, "too many uploads, retry later")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, files.MaxBytes+(1<<20))
	mr, err := r.MultipartReader()
	if err != nil {
		writeErr(w, http.StatusBadRequest, "multipart/form-data with a 'file' field required")
		return
	}
	var data []byte
	for {
		part, err := mr.NextPart()
		if err != nil {
			break
		}
		if part.FormName() != "file" {
			continue
		}
		data, err = io.ReadAll(io.LimitReader(part, files.MaxBytes+1))
		if err != nil {
			writeErr(w, http.StatusRequestEntityTooLarge, "file too large")
			return
		}
		break
	}
	if len(data) > files.MaxBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "file too large (max 5MB)")
		return
	}
	mime, wd, ht, err := files.Inspect(data)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	att := &store.Attachment{ID: store.RandHex(16), TenantID: tenantID, Uploader: uploader, Mime: mime, Size: len(data), Width: wd, Height: ht}
	if err := a.IM.Files.Put(r.Context(), att.ID, data); err != nil {
		serverErr(w, err)
		return
	}
	if err := a.St.InsertAttachment(r.Context(), att); err != nil {
		_ = a.IM.Files.Delete(r.Context(), att.ID)
		serverErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"file_id": att.ID, "url": "/files/" + att.ID, "width": wd, "height": ht, "size": att.Size, "mime": mime})
}

// serveFile：文件 ID 是 128 位随机数，充当"持有链接"（<img> 无法携带认证头）。
// 内容不可变，可长期缓存；响应头禁止浏览器嗅探类型并沙箱化，即使有人构造了图片/脚本混合文件也无法执行。
func (a *API) serveFile(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if a.IM.Files == nil || !files.ValidID(id) {
		http.NotFound(w, r)
		return
	}
	att, err := a.St.Attachment(r.Context(), id)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	etag := `"` + id + `"`
	h := w.Header()
	h.Set("ETag", etag)
	h.Set("Cache-Control", "private, max-age=31536000, immutable")
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	rc, err := a.IM.Files.Get(r.Context(), id)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer rc.Close()
	h.Set("Content-Type", att.Mime)
	h.Set("Content-Length", strconv.Itoa(att.Size))
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Security-Policy", "default-src 'none'; sandbox")
	h.Set("Content-Disposition", "inline")
	_, _ = io.Copy(w, rc)
}

func respondObj(w http.ResponseWriter, v any, err error) {
	if err != nil {
		serverErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

// ---- helpers ----

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json")
		return false
	}
	return true
}

func respond(w http.ResponseWriter, v any, err error) {
	if err != nil {
		serverErr(w, err)
		return
	}
	if v == nil || isNilSlice(v) {
		v = []any{}
	}
	writeJSON(w, http.StatusOK, v)
}

func isNilSlice(v any) bool {
	switch s := v.(type) {
	case []store.Account:
		return s == nil
	case []store.FAQ:
		return s == nil
	case []store.Session:
		return s == nil
	case []store.Message:
		return s == nil
	}
	return false
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func serverErr(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	log.Printf("api error: %v", err)
	writeErr(w, http.StatusInternalServerError, "internal error")
}

func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

var _ = context.Background
