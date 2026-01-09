package raft

import (
	"context"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source fsm.go -destination fsm_generated_test.go -package raft . FSM
type FSM[State any] interface {
	CreateSnapshot(ctx context.Context) ([]byte, error)
	RestoreSnapshot(ctx context.Context, leader uint64, data raftpb.Snapshot) error
	ApplyEntries(ctx context.Context, commands ...*Command) ([]ApplyResult, error)
	GetState() State
}

type ApplyResult struct {
	Result any
	Error error
}