package task

import (
	"context"
)

type ConnectorContext interface {
	Context() context.Context
	Scheduler() Scheduler
}

type ConnectorCtx struct {
	ctx       context.Context
	scheduler Scheduler
}

func (ctx *ConnectorCtx) Context() context.Context {
	return ctx.ctx
}

func (ctx *ConnectorCtx) Scheduler() Scheduler {
	return ctx.scheduler
}

func NewConnectorContext(ctx context.Context, scheduler Scheduler) *ConnectorCtx {
	return &ConnectorCtx{
		ctx:       ctx,
		scheduler: scheduler,
	}
}
