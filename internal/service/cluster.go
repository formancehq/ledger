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
	CreateLedger(ctx context.Context, name, driver string, config map[string]interface{}, metadata map[string]string, snapshotThreshold *uint64) (*ledgerpb.LedgerInfo, error)
	DeleteLedger(ctx context.Context, name string) error
	GetAllLedgersInfo(ctx context.Context) map[string]*ledgerpb.LedgerInfo
	GetLedgerInfo(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error)
	ResolveLedger(ctx context.Context, ledgerName string) (string, uint64, error)
	ResolveLedgerLeader(ctx context.Context, ledgerName string) (uint64, error)
}

type MasterCluster interface {
	Cluster
	System
	LeaderOnly
	GetClusterState(ctx context.Context) (*ledgerpb.ClusterState[ledgerpb.SystemState], error)
	GetLedgerCluster(ctx context.Context, name string) (LedgerCluster, error)
	GetLedgerClusterLocal(ctx context.Context, name string) (LedgerCluster, error)
}
