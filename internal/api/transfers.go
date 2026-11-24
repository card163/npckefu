package api

import (
	"errors"
	"net/http"
	"strconv"

	"npckefu/internal/im"
	"npckefu/internal/store"
)

func transferErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, im.ErrBadInput):
		writeErr(w, http.StatusBadRequest, "give at most one of to_agent / skill_group; note at most 500 characters; cannot transfer to the current agent")
	case errors.Is(err, im.ErrBadTarget):
		writeErr(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, im.ErrTargetUnavailable), errors.Is(err, im.ErrConflict), errors.Is(err, im.ErrClosed), errors.Is(err, im.ErrTooManyTransfers):
		writeErr(w, http.StatusConflict, err.Error())
	default:
		serverErr(w, err)
	}
}

// POST /v1/sessions/{id}/transfer  {"to_agent":"","skill_group":"","note":""}
// 租户服务端/主管代操作：不要求是当前接待坐席。三种用法：to_agent=指定坐席；skill_group=转到技能组；都不填=退回队列。
func (a *API) transferSession(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	var in struct {
		ToAgent    string `json:"to_agent"`
		SkillGroup string `json:"skill_group"`
		Note       string `json:"note"`
	}
	if !decode(w, r, &in) {
		return
	}
	s, err := a.IM.TransferAgent(r.Context(), t.ID, "", id, im.TransferReq{ToAgent: in.ToAgent, SkillGroup: in.SkillGroup, Note: in.Note})
	if err != nil {
		transferErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s)
}

func (a *API) listTransfers(w http.ResponseWriter, r *http.Request, t *store.Tenant) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if _, err := a.St.Session(r.Context(), t.ID, id); err != nil {
		serverErr(w, err)
		return
	}
	v, err := a.St.ListTransfers(r.Context(), t.ID, id)
	if err == nil && v == nil {
		v = []store.Transfer{}
	}
	respondObj(w, v, err)
}
