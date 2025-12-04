package service

import (
	"github.com/formancehq/ledger-v3-poc/api"
	"github.com/hashicorp/raft"
)

// ClusterClient provides access to Raft and gRPC client
type ClusterClient interface {
	GetRaft() *raft.Raft
	GetGRPCClient() GRPCClient
}

// GRPCClient provides access to the gRPC client
type GRPCClient interface {
	GetClient() api.LedgerServiceClient
}

