package api

import (
	"net/http"
	"strconv"
	"time"

	"npckefu/internal/store"
)

// GET /v1/tenant —— 校验凭证并返回租户基本信息（管理后台登录用；不返回 secret）。
func (a *API) tenantInfo(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	writeJSON(w, http.StatusOK, map[string]any{"id": t.ID, "name": t.Name, "app_key": t.AppKey})
}

// GET /v1/sessions/{id}
func (a *API) getSession(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	v, err := a.St.Session(r.Context(), t.ID, id)
	respondObj(w, v, err)
}

// GET /v1/stats/overview?since=RFC3339 —— 概览：实时会话数、坐席在线数，以及 since 起（默认最近 24 小时）的会话/消息/评价。
func (a *API) overview(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	since := time.Now().Add(-24 * time.Hour)
	if v := r.URL.Query().Get("since"); v != "" {
		x, ok := parseTime(v)
		if !ok {
			writeErr(w, http.StatusBadRequest, "since: RFC3339 or YYYY-MM-DD")
			return
		}
		since = x
	}
	ctx := r.Context()
	live, err := a.St.LiveCounts(ctx, t.ID)
	if err != nil {
		serverErr(w, err)
		return
	}
	sessions, human, messages, err := a.St.CountSince(ctx, t.ID, since)
	if err != nil {
		serverErr(w, err)
		return
	}
	roster, err := a.IM.Roster(ctx, t.ID)
	if err != nil {
		serverErr(w, err)
		return
	}
	online := 0
	for _, x := range roster {
		if x.Online {
			online++
		}
	}
	qs, err := a.St.QueueSettings(ctx, t.ID)
	if err != nil {
		serverErr(w, err)
		return
	}
	qCount, qLongest, qOverdue, err := a.St.QueueStats(ctx, t.ID, qs.AlertAfter)
	if err != nil {
		serverErr(w, err)
		return
	}
	rs, err := a.St.RatingStats(ctx, t.ID, since, time.Now().Add(time.Minute), "")
	if err != nil {
		serverErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"since": since, "live": live,
		"agents":   map[string]int{"total": len(roster), "online": online},
		"sessions": map[string]int{"total": sessions, "human": human},
		"messages": messages,
		"queue":    map[string]int{"count": qCount, "longest_wait": qLongest, "overdue": qOverdue, "alert_after": qs.AlertAfter},
		"rating":   map[string]any{"count": rs.Count, "avg": rs.Avg, "satisfaction_rate": rs.SatisfactionRate},
	})
}

// GET /v1/agents/roster —— 坐席名单：在线状态、当前接待数/上限、技能组。
func (a *API) roster(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	v, err := a.IM.Roster(r.Context(), t.ID)
	respondObj(w, v, err)
}

// POST /v1/sessions/{id}/close —— 强制结束会话（访客与坐席都会收到通知）。
func (a *API) closeSession(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err := a.IM.CloseByTenant(r.Context(), t.ID, id); err != nil {
		serverErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// PUT /v1/faqs/{id}
func (a *API) updateFAQ(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	var f store.FAQ
	if !decode(w, r, &f) || f.Question == "" || f.Answer == "" {
		writeErr(w, http.StatusBadRequest, "question and answer required")
		return
	}
	f.ID, _ = strconv.ParseInt(r.PathValue("id"), 10, 64)
	f.TenantID = t.ID
	if err := a.St.UpdateFAQ(r.Context(), &f); err != nil {
		serverErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, f)
}

// POST /v1/accounts/{accid}/token —— 重置账号登录令牌并立即踢掉旧令牌的所有连接（含其他节点）。
func (a *API) resetToken(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	accid := r.PathValue("accid")
	tok, err := a.St.ResetToken(r.Context(), t.ID, accid)
	if err != nil {
		serverErr(w, err)
		return
	}
	a.IM.Hub.Kick(t.ID, accid)
	writeJSON(w, http.StatusOK, map[string]string{"accid": accid, "token": tok})
}

// GET /v1/webhook/events?status=&before=&limit= —— 回调投递记录（不含载荷）。
func (a *API) webhookEvents(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	q := r.URL.Query()
	before, _ := strconv.ParseInt(q.Get("before"), 10, 64)
	limit, _ := strconv.Atoi(q.Get("limit"))
	v, err := a.St.ListWebhookEvents(r.Context(), t.ID, q.Get("status"), before, limit)
	if err == nil && v == nil {
		v = []store.WebhookEventRow{}
	}
	respondObj(w, v, err)
}

func (a *API) retryWebhookEvent(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err := a.St.RetryWebhookEvent(r.Context(), t.ID, id); err != nil {
		serverErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /v1/settings/queue —— 排队超时设置（没保存过时返回默认值，is_default=true）。
func (a *API) getQueueSettings(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	v, err := a.St.QueueSettings(r.Context(), t.ID)
	respondObj(w, v, err)
}

// PUT /v1/settings/queue —— 保存排队超时设置：到点提醒访客、超时告警、可选放弃排队。秒数为 0 表示关闭该功能。
func (a *API) putQueueSettings(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	var in store.QueueSettings
	if !decode(w, r, &in) {
		return
	}
	if err := in.Validate(); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := a.St.SaveQueueSettings(r.Context(), t.ID, in); err != nil {
		serverErr(w, err)
		return
	}
	in.IsDefault = false
	writeJSON(w, http.StatusOK, in)
}
