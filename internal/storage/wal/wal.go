package wal

import (
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source wal.go -destination wal_generated.go -typed -package wal . WAL
type WAL interface {
	raft.Storage
	CreateSnapshot(i uint64, r *raftpb.ConfState, data []byte) error
	UpdateSnapshotConfState(cs *raftpb.ConfState) error
	Compact(u uint64) error
	Append(state raftpb.HardState, entries []raftpb.Entry) error
	ApplySnapshot(snapshot raftpb.Snapshot) error
	// EnsureCommitDurable blocks until HardState.Commit >= target is durably
	// fsync'd. The FSM apply path calls this before committing a Pebble batch
	// to ensure FSM.applied never outruns the WAL's durable commit pointer —
	// if it did, a crash would leave the FSM ahead of raft and the next
	// startup would panic with "applied(N) is out of range".
	EnsureCommitDurable(target uint64)
	Close() error
}
