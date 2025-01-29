package leadership

import (
	"context"
)

type contextKey string

var holderContextKey contextKey = "holder"

func ContextWithLeadershipInfo(ctx context.Context) context.Context {
	return context.WithValue(ctx, holderContextKey, &holder{})
}

func IsLeader(ctx context.Context) bool {
	h := ctx.Value(holderContextKey)
	if h == nil {
		return false
	}
	return h.(*holder).isLeader
}

func setIsLeader(ctx context.Context, isLeader bool) {
	h := ctx.Value(holderContextKey)
	if h == nil {
		return
	}
	h.(*holder).isLeader = isLeader
}

type holder struct {
	isLeader bool
}
