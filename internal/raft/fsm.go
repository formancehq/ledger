package raft

import (
	"context"
)

type FSM[State any] interface {
	CreateSnapshot(ctx context.Context) ([]byte, error)
	RestoreSnapshot(ctx context.Context, data []byte)
	ApplyEntries(ctx context.Context, commands ...Command) []ApplyResult
	GetState() State
}

type ApplyResult struct {
	Result any
	Error error
}