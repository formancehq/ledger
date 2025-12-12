package raft

import (
	"context"
)

type FSM interface {
	CreateSnapshot(ctx context.Context) ([]byte, error)
	RestoreSnapshot(ctx context.Context, data []byte) error
	ApplyEntries(ctx context.Context, commands ...Command) []ApplyResult
}

type ApplyResult struct {
	Result any
	Error error
}