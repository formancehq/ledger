package service

import (
	"github.com/formancehq/ledger-v3-poc/api"
	"go.etcd.io/etcd/raft/v3"
)

// ClusterClient provides access to Raft and gRPC client
type ClusterClient interface {
	GetRaft() *raft.RawNode
	GetGRPCClient() GRPCClient
}

// GRPCClient provides access to the gRPC client
type GRPCClient interface {
	GetClient() api.LedgerServiceClient
}
