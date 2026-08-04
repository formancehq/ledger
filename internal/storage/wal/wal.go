package wal

import (
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

//go:generate go tool mockgen -write_source_comment=false -write_package_comment=false -source wal.go -destination wal_generated.go -typed -package wal . WAL
type WAL interface {
	raft.Storage
	CreateSnapshot(i uint64, r *raftpb.ConfState, data []byte) error
	UpdateSnapshotConfState(cs *raftpb.ConfState) error
	Compact(u uint64) error
	Append(state *raftpb.HardState, entries []*raftpb.Entry) error
	ApplySnapshot(snapshot *raftpb.Snapshot) error
	Close() error
	// MarkClusterJoined creates the CLUSTER_JOINED marker in the WAL data
	// directory. The marker proves this node has been accepted by the
	// cluster — it is consumed by the operator's StatefulSet entrypoint to
	// decide whether a non-zero pod must restart with --join (when the
	// marker is absent) or as a pure restart (when present). It is written
	// after the initial bootstrap snapshot lands for pod-0, and after the
	// leader accepts the JoinAsLearner RPC for the other pods.
	MarkClusterJoined() error
}
