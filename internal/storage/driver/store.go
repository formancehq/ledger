package driver

import (
	"context"

	"github.com/formancehq/go-libs/v4/metadata"
	"github.com/formancehq/go-libs/v4/migrations"

	ledger "github.com/formancehq/ledger/internal"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package driver_test . SystemStore

type SystemStore interface {
	CreateLedger(ctx context.Context, l *ledger.Ledger) error
	DeleteLedgerMetadata(ctx context.Context, name string, key string) error
	UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error
	//ListLedgers(ctx context.Context, q systemstore.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error)
	GetLedger(ctx context.Context, name string) (*ledger.Ledger, error)
	GetDistinctBuckets(ctx context.Context) ([]string, error)
	CountLedgersInBucket(ctx context.Context, bucket string) (int, error)

	Migrate(ctx context.Context, options ...migrations.Option) error
	GetMigrator(options ...migrations.Option) *migrations.Migrator
	IsUpToDate(ctx context.Context) (bool, error)
}
