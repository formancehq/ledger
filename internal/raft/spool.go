package raft

import (
	"context"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

type Position struct {
	SegID  uint64
	Offset int64
}

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source spool.go -destination spool_generated_test.go -typed -package raft . Spool
type Spool interface {
	AppendCommittedEntries(ctx context.Context, entries ...raftpb.Entry) error
	End() (*Position, error)
	ReplayUntil(
		ctx context.Context,
		end Position,
		lastApplied uint64,
		applyFn func(raftpb.Entry) error) error
	Prune(lastApplied uint64) error
	Close() error
}
