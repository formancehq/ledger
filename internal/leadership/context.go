package leadership

import (
	"context"
	"sync"
)

type contextKey struct{}

var holderContextKey contextKey = struct{}{}

func ContextWithLeadershipInfo(ctx context.Context) context.Context {
	return context.WithValue(ctx, holderContextKey, &holder{})
}

func IsLeader(ctx context.Context) bool {
	h := ctx.Value(holderContextKey)
	if h == nil {
		return false
	}
	holder := h.(*holder)
	holder.Lock()
	defer holder.Unlock()

	return holder.isLeader
}

func setIsLeader(ctx context.Context, isLeader bool) {
	h := ctx.Value(holderContextKey)
	if h == nil {
		return
	}
	holder := h.(*holder)
	holder.Lock()
	defer holder.Unlock()

	holder.isLeader = isLeader
}

type holder struct {
	sync.Mutex
	isLeader bool
}
