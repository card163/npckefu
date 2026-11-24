package api

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"npckefu/internal/im"
	"npckefu/internal/store"
)

// rateErr 把评价相关的业务错误映射为 HTTP 状态码。
func rateErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, im.ErrBadInput):
		writeErr(w, http.StatusBadRequest, "score must be 1-5 and comment at most 500 characters")
	case errors.Is(err, im.ErrSensitive):
		writeErr(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, im.ErrNotClosed), errors.Is(err, im.ErrAlreadyRated):
		writeErr(w, http.StatusConflict, err.Error())
	case errors.Is(err, im.ErrRateExpired):
		writeErr(w, http.StatusGone, err.Error())
	default:
		serverErr(w, err)
	}
}

// POST /v1/sessions/{id}/rating  {"score":1-5,"comment":"…"} —— 租户服务端代访客提交评价。
func (a *API) rateSession(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	var in struct {
		Score   int    `json:"score"`
		Comment string `json:"comment"`
	}
	if !decode(w, r, &in) {
		return
	}
	rt, err := a.IM.RateOnBehalf(r.Context(), t.ID, id, in.Score, in.Comment)
	if err != nil {
		rateErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, rt)
}

func (a *API) getRating(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	v, err := a.St.Rating(r.Context(), t.ID, id)
	respondObj(w, v, err)
}

// GET /v1/ratings?agent=&min_score=&max_score=&before=&limit=  最近的评价（按会话 ID 倒序，before 翻页）。
func (a *API) listRatings(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	q := r.URL.Query()
	atoi := func(k string) int { n, _ := strconv.Atoi(q.Get(k)); return n }
	before, _ := strconv.ParseInt(q.Get("before"), 10, 64)
	v, err := a.St.ListRatings(r.Context(), t.ID, store.RatingFilter{
		Agent: q.Get("agent"), MinScore: atoi("min_score"), MaxScore: atoi("max_score"), BeforeID: before, Limit: atoi("limit"),
	})
	if err == nil && v == nil {
		v = []store.Rating{}
	}
	respondObj(w, v, err)
}

// parseTime 接受 RFC3339 或 YYYY-MM-DD（按 UTC 的 0 点）。
func parseTime(s string) (time.Time, bool) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, true
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, true
	}
	return time.Time{}, false
}

// GET /v1/stats/ratings?from=&to=&agent=  统计区间 [from,to) 内的评价；默认最近 30 天。
// to 为纯日期（YYYY-MM-DD）时包含当天。
func (a *API) ratingStats(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	q := r.URL.Query()
	to, from := time.Now().Add(time.Minute), time.Time{}
	if v := q.Get("to"); v != "" {
		x, ok := parseTime(v)
		if !ok {
			writeErr(w, http.StatusBadRequest, "to: RFC3339 or YYYY-MM-DD")
			return
		}
		if len(v) == len("2006-01-02") {
			x = x.Add(24 * time.Hour)
		}
		to = x
	}
	if v := q.Get("from"); v != "" {
		x, ok := parseTime(v)
		if !ok {
			writeErr(w, http.StatusBadRequest, "from: RFC3339 or YYYY-MM-DD")
			return
		}
		from = x
	} else {
		from = to.Add(-30 * 24 * time.Hour)
	}
	st, err := a.St.RatingStats(r.Context(), t.ID, from, to, q.Get("agent"))
	respondObj(w, st, err)
}
