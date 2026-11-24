package im

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"npckefu/internal/store"
)

var (
	ErrNotClosed    = errors.New("会话结束后才能评价")
	ErrRateExpired  = errors.New("评价已超过有效期")
	ErrAlreadyRated = errors.New("该会话已评价")
)

const (
	// RatingWindow 会话结束后多久内可以评价。
	RatingWindow    = 7 * 24 * time.Hour
	MaxCommentRunes = 500
)

// Rate 访客对自己的已结束会话评价（1~5 分 + 可选评语）。
func (s *Service) Rate(ctx context.Context, acc *store.Account, sessionID int64, score int, comment string) (*store.Rating, error) {
	if acc.Role != store.RoleVisitor {
		return nil, ErrForbidden
	}
	sess, err := s.St.Session(ctx, acc.TenantID, sessionID)
	if err != nil {
		return nil, err
	}
	if sess.VisitorAccid != acc.Accid {
		return nil, ErrForbidden
	}
	return s.rate(ctx, sess, score, comment)
}

// RateOnBehalf 租户服务端代访客提交评价（如评价入口在租户自己的 App 里）。
func (s *Service) RateOnBehalf(ctx context.Context, tenantID, sessionID int64, score int, comment string) (*store.Rating, error) {
	sess, err := s.St.Session(ctx, tenantID, sessionID)
	if err != nil {
		return nil, err
	}
	return s.rate(ctx, sess, score, comment)
}

func (s *Service) rate(ctx context.Context, sess *store.Session, score int, comment string) (*store.Rating, error) {
	if score < 1 || score > 5 {
		return nil, ErrBadInput
	}
	comment = strings.TrimSpace(comment)
	if utf8.RuneCountInString(comment) > MaxCommentRunes {
		return nil, ErrBadInput
	}
	if sess.Status != store.StatusClosed {
		return nil, ErrNotClosed
	}
	if sess.ClosedAt != nil && time.Since(*sess.ClosedAt) > RatingWindow {
		return nil, ErrRateExpired
	}
	if comment != "" { // 评语同样过敏感词：命中"拒绝"词则拒收，命中"屏蔽"词则替换
		if r := s.Mod.Check(ctx, sess.TenantID, comment); r.Hit {
			if r.Block {
				return nil, ErrSensitive
			}
			comment = r.Text
		}
	}
	r := &store.Rating{SessionID: sess.ID, VisitorAccid: sess.VisitorAccid, AgentAccid: sess.AgentAccid, Score: score, Comment: comment}
	created, err := s.St.InsertRating(ctx, sess.TenantID, r)
	if err != nil {
		return nil, err
	}
	if !created {
		return nil, ErrAlreadyRated
	}
	if saved, err := s.St.Rating(ctx, sess.TenantID, sess.ID); err == nil {
		r = saved
	}
	s.emit(sess.TenantID, "session.rated", map[string]any{"session": sess, "rating": r})
	if sess.AgentAccid != "" { // 通知当时接待的坐席
		s.Hub.Push(sess.TenantID, sess.AgentAccid, Event{Op: "rated", SessionID: sess.ID, Score: r.Score, Comment: r.Comment, From: sess.VisitorAccid})
	}
	return r, nil
}
