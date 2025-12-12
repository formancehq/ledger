package service

import (
	"context"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

type SystemWriter interface {
	LeaderOnly
	CreateBucket(ctx context.Context, name, driver string, config map[string]interface{}, snapshotThreshold *uint64) (*ledger.BucketInfo, error)
	DeleteBucket(ctx context.Context, name string) error
}
