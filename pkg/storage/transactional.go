package storage

import (
	"context"
)

type contextHolder struct {
	transactional bool
	kv            map[any]any
}

type contextHolderKeyStruct struct{}

var contextKey = contextHolderKeyStruct{}

func withContextHolder(ctx context.Context, holder *contextHolder) context.Context {
	return context.WithValue(ctx, contextKey, holder)
}

func TransactionalContext(ctx context.Context) context.Context {
	return withContextHolder(ctx, &contextHolder{
		transactional: true,
		kv:            map[any]any{},
	})
}
