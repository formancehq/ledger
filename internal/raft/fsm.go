package raft

import (
	"context"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

type FSM[State any] interface {
	CreateSnapshot(ctx context.Context) ([]byte, error)
	RestoreSnapshot(ctx context.Context, leader uint64, data raftpb.Snapshot)
	ApplyEntries(ctx context.Context, commands ...Command) ([]ApplyResult, error)
	GetState() State
}

type ApplyResult struct {
	Result any
	Error error
}