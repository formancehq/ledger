package wal

import (
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source wal.go -destination wal_generated.go -typed -package wal . WAL
type WAL interface {
	raft.Storage
	CreateSnapshot(i uint64, r *raftpb.ConfState, data []byte) error
	Compact(u uint64) error
	Append(state raftpb.HardState, entries []raftpb.Entry) error
	ApplySnapshot(snapshot raftpb.Snapshot) error
	Close() error
}
