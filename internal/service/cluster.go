package service

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

type LeaderOnly interface {
	Snapshot(ctx context.Context) error
}

type Cluster interface {
	IsHealthy() bool
	GetLeader() uint64
}

type LedgerCluster interface {
	LeaderOnly
	Cluster
	Ledger
	GetClusterState(ctx context.Context) (*ledgerpb.ClusterState[ledgerpb.LedgerState], error)
}

type System interface {
	LeaderOnly
	CreateLedger(ctx context.Context, name string, logStoreConfig, runtimeStoreConfig map[string]interface{}, metadata map[string]string, snapshotThreshold *uint64, logStoreDriver, runtimeStoreDriver string) (*ledgerpb.LedgerInfo, error)
	DeleteLedger(ctx context.Context, name string) error
	GetAllLedgersInfo(ctx context.Context) (map[string]*ledgerpb.LedgerInfo, error)
	GetLedgerInfo(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error)
	ResolveLedger(ctx context.Context, ledgerName string) (string, uint64, error)
	ResolveLedgerLeader(ctx context.Context, ledgerName string) (uint64, error)
}

type MasterCluster interface {
	Cluster
	System
	LeaderOnly
	GetClusterState(ctx context.Context) (*ledgerpb.ClusterState[ledgerpb.SystemState], error)
	// todo: only used by api, we can probably relax the interface
	GetLedgerCluster(ctx context.Context, name string) (LedgerCluster, error)
	GetLedgerClusterLocal(ctx context.Context, name string) (LedgerCluster, error)
}
