package service

import (
	"context"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
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
	GetClusterState(ctx context.Context) (*ledger.ClusterState[ledger.LedgerState], error)
}

type System interface {
	LeaderOnly
	CreateLedger(ctx context.Context, name, driver string, config map[string]interface{}, metadata map[string]string, snapshotThreshold *uint64) (*ledger.LedgerInfo, error)
	DeleteLedger(ctx context.Context, name string) error
	GetAllLedgersInfo(ctx context.Context) map[string]ledger.LedgerInfo
	GetLedgerInfo(ctx context.Context, name string) (*ledger.LedgerInfo, error)
	ResolveLedger(ctx context.Context, ledgerName string) (string, uint64, error)
	ResolveLedgerLeader(ctx context.Context, ledgerName string) (uint64, error)
}

type MasterCluster interface {
	Cluster
	System
	LeaderOnly
	GetClusterState(ctx context.Context) (*ledger.ClusterState[ledger.SystemState], error)
	GetLedgerCluster(ctx context.Context, name string) (LedgerCluster, error)
	GetLedgerClusterLocal(ctx context.Context, name string) (LedgerCluster, error)
}
