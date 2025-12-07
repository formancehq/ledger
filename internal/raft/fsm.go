package raft

import (
	"context"
)

type FSM interface {
	CreateSnapshot(ctx context.Context) ([]byte, error)
	RestoreSnapshot(ctx context.Context, data []byte) error
	ApplyEntry(ctx context.Context, command Command) (any, error)
}
