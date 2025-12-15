package service

import (
	"context"

	"github.com/formancehq/go-libs/v3/metadata"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

type LeaderOnly interface {
	Snapshot(ctx context.Context) error
}

type Cluster interface {
	IsHealthy() bool
}

type BucketCluster interface {
	LeaderOnly
	Cluster
	Bucket
}

type Bucket interface {
	Ledger
	GetClusterState(ctx context.Context) (*ledger.ClusterState[ledger.BucketState], error)
	CreateLedger(ctx context.Context, name string, metadata metadata.Metadata) (*ledger.LedgerInfo, error)
	GetLedger(ctx context.Context, name string) (*ledger.LedgerInfo, error)
	GetLedgers(ctx context.Context) ([]ledger.LedgerInfo, error)
	GetAllLogs(ctx context.Context, from uint64) (Cursor[ledger.Log], error)
}

type System interface {
	CreateBucket(ctx context.Context, name, driver string, config map[string]interface{}, snapshotThreshold *uint64) (*ledger.BucketInfo, error)
	DeleteBucket(ctx context.Context, name string) error
	GetAllBucketsInfo(ctx context.Context) map[string]ledger.BucketInfo
	GetBucketInfo(ctx context.Context, name string) (*ledger.BucketInfo, error)
	ResolveLedger(ctx context.Context, ledgerName string) (string, uint64, error)
}

type MasterCluster interface {
	Cluster
	System
	LeaderOnly
	GetClusterState(ctx context.Context) (*ledger.ClusterState[ledger.SystemState], error)
	GetBucketCluster(ctx context.Context, name string) (BucketCluster, error)
	GetBucketClusterLocal(ctx context.Context, name string) (BucketCluster, error)
}
