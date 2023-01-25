package api

import (
	"context"
)

var contextKey = struct{}{}

type contextHolder struct {
	port int
}

func NewContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKey, &contextHolder{})
}

func setPort(ctx context.Context, port int) {
	v := ctx.Value(contextKey)
	if v == nil {
		return
	}
	v.(*contextHolder).port = port
}

func ListeningPort(ctx context.Context) int {
	v := ctx.Value(contextKey)
	if v == nil {
		return 0
	}
	return v.(*contextHolder).port
}
