package driver

import (
	"context"

	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/metadata"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	systemstore "github.com/formancehq/ledger/internal/storage/system"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

type DefaultStorageDriverAdapter struct {
	d *Driver
}

func (d *DefaultStorageDriverAdapter) GetLedger(ctx context.Context, name string) (*ledger.Ledger, error) {
	return systemstore.New(d.d.db).GetLedger(ctx, name)
}

func (d *DefaultStorageDriverAdapter) ListLedgers(ctx context.Context, query ledgercontroller.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error) {
	return systemstore.New(d.d.db).ListLedgers(ctx, query)
}

func (d *DefaultStorageDriverAdapter) UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error {
	return systemstore.New(d.d.db).UpdateLedgerMetadata(ctx, name, m)
}

func (d *DefaultStorageDriverAdapter) DeleteLedgerMetadata(ctx context.Context, param string, key string) error {
	return systemstore.New(d.d.db).DeleteLedgerMetadata(ctx, param, key)
}

func (d *DefaultStorageDriverAdapter) OpenLedger(ctx context.Context, name string) (ledgercontroller.Store, *ledger.Ledger, error) {
	store, l, err := d.d.OpenLedger(ctx, name)
	if err != nil {
		return nil, nil, err
	}

	return ledgerstore.NewDefaultStoreAdapter(store), l, nil
}

func (d *DefaultStorageDriverAdapter) CreateLedger(ctx context.Context, l *ledger.Ledger) error {
	_, err := d.d.CreateLedger(ctx, l)
	return err
}

func NewControllerStorageDriverAdapter(d *Driver) *DefaultStorageDriverAdapter {
	return &DefaultStorageDriverAdapter{d: d}
}

var _ systemcontroller.Store = (*DefaultStorageDriverAdapter)(nil)
