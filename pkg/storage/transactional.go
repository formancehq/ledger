package storage

import (
	"context"
	"errors"
)

type contextHolder struct {
	transactional bool
	transaction   any
	commit        func(ctx context.Context) error
}

type contextHolderKeyStruct struct{}

var contextKey = contextHolderKeyStruct{}

func withContextHolder(ctx context.Context, holder *contextHolder) context.Context {
	return context.WithValue(ctx, contextKey, holder)
}

func getContextHolder(ctx context.Context) *contextHolder {
	ctxHolder := ctx.Value(contextKey)
	if ctxHolder == nil {
		return nil
	}
	return ctxHolder.(*contextHolder)
}

func RegisteredTransaction(ctx context.Context) any {
	holder := getContextHolder(ctx)
	if holder == nil {
		panic("no context holder")
	}
	return holder.transaction
}

func RegisterTransaction(ctx context.Context, transaction any, commitFn func(ctx context.Context) error) {
	holder := getContextHolder(ctx)
	if holder == nil {
		panic("no context holder")
	}
	holder.transaction = transaction
	holder.commit = commitFn
}

func IsTransactionRegistered(ctx context.Context) bool {
	ctxHolder := ctx.Value(contextKey)
	if ctxHolder == nil {
		return false
	}
	return ctxHolder.(*contextHolder).transaction != nil
}

func IsTransactional(ctx context.Context) bool {
	ctxHolder := ctx.Value(contextKey)
	if ctxHolder == nil {
		return false
	}
	return ctxHolder.(*contextHolder).transactional
}

func TransactionalContext(ctx context.Context) context.Context {
	return withContextHolder(ctx, &contextHolder{
		transactional: true,
	})
}

func CommitTransaction(ctx context.Context) error {
	holder := getContextHolder(ctx)
	if holder == nil {
		panic("context holder is nil")
	}
	if holder.transaction == nil {
		return errors.New("transaction not initialized")
	}
	return holder.commit(ctx)
}
