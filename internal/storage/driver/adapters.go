package driver

import (
	"context"

	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
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

func (d *DefaultStorageDriverAdapter) MarkBucketAsDeleted(ctx context.Context, bucketName string) error {
	return d.Driver.MarkBucketAsDeleted(ctx, bucketName)
}

func (d *DefaultStorageDriverAdapter) GetDistinctBuckets(ctx context.Context) ([]string, error) {
	return d.Driver.GetDistinctBuckets(ctx)
}

func (d *DefaultStorageDriverAdapter) GetLedgersByBucket(ctx context.Context, bucketName string) ([]ledger.Ledger, error) {
	return d.Driver.GetLedgersByBucket(ctx, bucketName)
}

func NewControllerStorageDriverAdapter(d *Driver) *DefaultStorageDriverAdapter {
	return &DefaultStorageDriverAdapter{Driver: d}
}

var _ systemcontroller.Store = (*DefaultStorageDriverAdapter)(nil)
