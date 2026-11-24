package api

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"npckefu/internal/store"
)

const maxReportDays = 370

type agentRow struct {
	store.AgentWorkload
	Name       string `json:"name"`
	SkillGroup string `json:"skill_group"`
	Online     bool   `json:"online"`
	Load       int    `json:"load"`
	MaxLoad    int    `json:"max_load"`
}

type workloadTotals struct {
	Sessions         int      `json:"sessions"`    // 全体去重会话数
	Assignments      int      `json:"assignments"` // 接待次数合计
	Replied          int      `json:"replied"`
	AvgFirstReply    *float64 `json:"avg_first_reply_seconds"`
	AvgHandle        *float64 `json:"avg_handle_seconds"`
	Messages         int      `json:"messages"`
	Transfers        int      `json:"transfers"`
	Ratings          int      `json:"ratings"`
	AvgRating        *float64 `json:"avg_rating"`
	SatisfactionRate *float64 `json:"satisfaction_rate"`
}

func r1(f float64) *float64 { v := float64(int(f*10+0.5)) / 10; return &v }

// GET /v1/stats/agents —— 坐席工作量报表。
// 参数：from、to（RFC3339 或 YYYY-MM-DD，纯日期按 tz 解释，to 含当天；默认最近 7 天，最长 370 天）、tz、agent、format=json|csv。
// 接待按"接到会话的时间"归属；消息、评价按各自发生的时间。只统计该功能上线后产生的接待（不回填历史）。
func (a *API) agentStats(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	q := r.URL.Query()
	loc := time.UTC
	if tz := q.Get("tz"); tz != "" {
		var err error
		if loc, err = time.LoadLocation(tz); err != nil {
			writeErr(w, http.StatusBadRequest, "tz: unknown IANA time zone, e.g. Asia/Shanghai")
			return
		}
	}
	format := q.Get("format")
	if format != "" && format != "json" && format != "csv" {
		writeErr(w, http.StatusBadRequest, "format: json or csv")
		return
	}
	from, to, err := parseRange(q, loc, 7)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if to.Sub(from) > maxReportDays*24*time.Hour {
		writeErr(w, http.StatusBadRequest, fmt.Sprintf("range too long (max %d days)", maxReportDays))
		return
	}
	agent := q.Get("agent")
	ctx := r.Context()
	loads, err := a.St.AgentWorkloads(ctx, t.ID, from, to, agent)
	if err != nil {
		serverErr(w, err)
		return
	}
	roster, err := a.IM.Roster(ctx, t.ID)
	if err != nil {
		serverErr(w, err)
		return
	}
	byAcc := map[string]*agentRow{}
	var rows []*agentRow
	add := func(wl store.AgentWorkload) *agentRow {
		x := &agentRow{AgentWorkload: wl}
		byAcc[wl.AgentAccid] = x
		rows = append(rows, x)
		return x
	}
	for _, wl := range loads {
		add(wl)
	}
	for _, ro := range roster { // 名单里的坐席即使这段时间没活动也列出来（全 0），一眼看出谁没在工作
		if agent != "" && ro.Accid != agent {
			continue
		}
		x := byAcc[ro.Accid]
		if x == nil {
			x = add(store.AgentWorkload{AgentAccid: ro.Accid})
		}
		x.Name, x.SkillGroup, x.Online, x.Load, x.MaxLoad = ro.Name, ro.SkillGroup, ro.Online, ro.Load, ro.MaxLoad
	}

	// 合计：平均值按各自的样本数加权
	var tot workloadTotals
	var fSum, fN, hSum, hN, rSum float64
	var good int
	for _, x := range rows {
		tot.Replied += x.Replied
		tot.Messages += x.Messages
		tot.Transfers += x.TransferredOut
		tot.Ratings += x.Ratings
		if x.AvgFirstReply != nil {
			fSum, fN = fSum+*x.AvgFirstReply*float64(x.Replied), fN+float64(x.Replied)
		}
		if x.AvgHandle != nil {
			hSum, hN = hSum+*x.AvgHandle*float64(x.Ended), hN+float64(x.Ended)
		}
		if x.AvgRating != nil {
			rSum += *x.AvgRating * float64(x.Ratings)
		}
		if x.SatisfactionRate != nil {
			good += int(*x.SatisfactionRate*float64(x.Ratings) + 0.5)
		}
	}
	if tot.Sessions, tot.Assignments, err = a.St.WorkloadTotals(ctx, t.ID, from, to, agent); err != nil {
		serverErr(w, err)
		return
	}
	if fN > 0 {
		tot.AvgFirstReply = r1(fSum / fN)
	}
	if hN > 0 {
		tot.AvgHandle = r1(hSum / hN)
	}
	if tot.Ratings > 0 {
		tot.AvgRating = r1(rSum / float64(tot.Ratings))
		v := float64(int(float64(good)/float64(tot.Ratings)*1000+0.5)) / 1000
		tot.SatisfactionRate = &v
	}

	if format == "csv" {
		writeAgentCSV(w, rows, from, to, loc)
		return
	}

	// 按天汇总并补全没有数据的日期，图表的横轴才是连续的
	_, off := from.In(loc).Zone()
	offset := fmt.Sprintf("%+03d:%02d", off/3600, (abs(off)%3600)/60)
	byDay, err := a.St.WorkloadDaily(ctx, t.ID, from, to, agent, offset)
	if err != nil {
		serverErr(w, err)
		return
	}
	daily := []store.DayStat{}
	d := from.In(loc)
	d = time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, loc)
	for d.Before(to) {
		k := d.Format("2006-01-02")
		if x := byDay[k]; x != nil {
			daily = append(daily, *x)
		} else {
			daily = append(daily, store.DayStat{Date: k})
		}
		d = d.AddDate(0, 0, 1)
	}
	if rows == nil {
		rows = []*agentRow{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"from": from, "to": to, "tz": loc.String(), "agents": rows, "totals": tot, "daily": daily})
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

func writeAgentCSV(w http.ResponseWriter, rows []*agentRow, from, to time.Time, loc *time.Location) {
	h := w.Header()
	h.Set("Content-Type", "text/csv; charset=utf-8")
	h.Set("Content-Disposition", `attachment; filename="agent-workload-`+from.In(loc).Format("20060102")+`-`+to.Add(-time.Second).In(loc).Format("20060102")+`.csv"`)
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	w.Write([]byte("\xEF\xBB\xBF")) // UTF-8 BOM：Excel 才会正确识别中文
	cw := csv.NewWriter(w)
	cw.Write([]string{"agent_accid", "name", "skill_group", "sessions", "assignments", "replied", "avg_first_reply_seconds", "max_first_reply_seconds", "avg_handle_seconds",
		"messages", "transferred_in", "transferred_out", "ratings", "avg_rating", "satisfaction_rate"})
	f := func(p *float64) string {
		if p == nil {
			return ""
		}
		return strconv.FormatFloat(*p, 'f', -1, 64)
	}
	n := strconv.Itoa
	for _, x := range rows {
		cw.Write([]string{safeCell(x.AgentAccid), safeCell(x.Name), safeCell(x.SkillGroup), n(x.Sessions), n(x.Assignments), n(x.Replied), f(x.AvgFirstReply), f(x.MaxFirstReply), f(x.AvgHandle),
			n(x.Messages), n(x.TransferredIn), n(x.TransferredOut), n(x.Ratings), f(x.AvgRating), f(x.SatisfactionRate)})
	}
	cw.Flush()
}
