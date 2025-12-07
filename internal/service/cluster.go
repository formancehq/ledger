package service

import (
	"go.etcd.io/etcd/raft/v3"
)

// ClusterClient provides access to Raft and gRPC client
type ClusterClient interface {
	GetRaft() *raft.RawNode
	GetLeaderGRPCClient() SystemServiceClient
	GetLeaderLedgerGRPCClient() LedgerServiceClient
}
