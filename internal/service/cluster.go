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
	GetClusterState(ctx context.Context) (*ledger.ClusterState, error)
	IsHealthy() bool
}

type BucketCluster interface {
	Cluster
	BucketReader
	BucketWriter
}

type BucketReader interface {
	Info() ledger.BucketInfo
	GetLedger(ctx context.Context, name string) (*ledger.LedgerInfo, error)
	GetLedgers(ctx context.Context) ([]ledger.LedgerInfo, error)
}

type BucketWriter interface {
	Ledger
	LeaderOnly
	CreateLedger(ctx context.Context, name string, metadata metadata.Metadata) (*ledger.LedgerInfo, error)
}

type SystemReader interface {
	GetAllBuckets() map[string]BucketCluster
	GetBucket(ctx context.Context, name string) (BucketCluster, error)
	GetBucketOfLedger(ctx context.Context, ledgerName string) (BucketCluster, error)
}

type MasterCluster interface {
	Cluster
	SystemWriter
	SystemReader
}
