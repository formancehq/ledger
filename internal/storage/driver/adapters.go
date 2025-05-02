package driver

import (
	"context"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/storage/common"
)

type DefaultStorageDriverAdapter struct {
	*Driver
}

func (d *DefaultStorageDriverAdapter) OpenLedger(ctx context.Context, name string) (ledgercontroller.Store, *ledger.Ledger, error) {
	store, l, err := d.Driver.OpenLedger(ctx, name)
	if err != nil {
		return nil, nil, err
	}

	return ledgerstore.NewDefaultStoreAdapter(store), l, nil
}

func (d *DefaultStorageDriverAdapter) CreateLedger(ctx context.Context, l *ledger.Ledger) error {
	_, err := d.Driver.CreateLedger(ctx, l)
	return err
}

func (d *DefaultStorageDriverAdapter) ListBucketsWithStatus(ctx context.Context, query common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[systemcontroller.BucketWithStatus], error) {
	return d.Driver.ListBucketsWithStatus(ctx, query)
}

func (d *DefaultStorageDriverAdapter) MarkBucketAsDeleted(ctx context.Context, bucketName string) error {
	return d.Driver.MarkBucketAsDeleted(ctx, bucketName)
}

func (d *DefaultStorageDriverAdapter) RestoreBucket(ctx context.Context, bucketName string) error {
	return d.Driver.RestoreBucket(ctx, bucketName)
}

func NewControllerStorageDriverAdapter(d *Driver) *DefaultStorageDriverAdapter {
	return &DefaultStorageDriverAdapter{Driver: d}
}

var _ systemcontroller.Store = (*DefaultStorageDriverAdapter)(nil)
