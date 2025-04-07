package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/migrations"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/pkg/features"
	"go.opentelemetry.io/otel/metric"
	noopmetrics "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"

	"errors"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/uptrace/bun"
)

type Store struct {
	db     bun.IDB
	bucket bucket.Bucket
	ledger ledger.Ledger

	tracer                             trace.Tracer
	meter                              metric.Meter
	checkBucketSchemaHistogram         metric.Int64Histogram
	checkLedgerSchemaHistogram         metric.Int64Histogram
	updateAccountsMetadataHistogram    metric.Int64Histogram
	deleteAccountMetadataHistogram     metric.Int64Histogram
	upsertAccountsHistogram            metric.Int64Histogram
	getBalancesHistogram               metric.Int64Histogram
	insertLogHistogram                 metric.Int64Histogram
	readLogWithIdempotencyKeyHistogram metric.Int64Histogram
	insertMovesHistogram               metric.Int64Histogram
	insertTransactionHistogram         metric.Int64Histogram
	revertTransactionHistogram         metric.Int64Histogram
	updateTransactionMetadataHistogram metric.Int64Histogram
	deleteTransactionMetadataHistogram metric.Int64Histogram
	updateBalancesHistogram            metric.Int64Histogram
	getVolumesWithBalancesHistogram    metric.Int64Histogram
}

func (store *Store) Volumes() common.PaginatedResource[
	ledger.VolumesWithBalanceByAssetByAccount,
	ledgercontroller.GetVolumesOptions,
	common.OffsetPaginatedQuery[ledgercontroller.GetVolumesOptions]] {
	return common.NewPaginatedResourceRepository(&volumesResourceHandler{store: store}, common.OffsetPaginator[ledger.VolumesWithBalanceByAssetByAccount, ledgercontroller.GetVolumesOptions]{
		DefaultPaginationColumn: "account",
		DefaultOrder:            bunpaginate.OrderAsc,
	})
}

func (store *Store) AggregatedVolumes() common.Resource[ledger.AggregatedVolumes, ledgercontroller.GetAggregatedVolumesOptions] {
	return common.NewResourceRepository[ledger.AggregatedVolumes, ledgercontroller.GetAggregatedVolumesOptions](&aggregatedBalancesResourceRepositoryHandler{
		store: store,
	})
}

func (store *Store) Transactions() common.PaginatedResource[
	ledger.Transaction,
	any,
	common.ColumnPaginatedQuery[any]] {
	return common.NewPaginatedResourceRepository(&transactionsResourceHandler{store: store}, common.ColumnPaginator[ledger.Transaction, any]{
		DefaultPaginationColumn: "id",
		DefaultOrder:            bunpaginate.OrderDesc,
	})
}

func (store *Store) Logs() common.PaginatedResource[
	ledger.Log,
	any,
	common.ColumnPaginatedQuery[any]] {
	return common.NewPaginatedResourceRepositoryMapper[ledger.Log, Log, any, common.ColumnPaginatedQuery[any]](&logsResourceHandler{
		store: store,
	}, common.ColumnPaginator[Log, any]{
		DefaultPaginationColumn: "id",
		DefaultOrder:            bunpaginate.OrderDesc,
	})
}

func (store *Store) Accounts() common.PaginatedResource[
	ledger.Account,
	any,
	common.OffsetPaginatedQuery[any]] {
	return common.NewPaginatedResourceRepository(&accountsResourceHandler{
		store: store,
	}, common.OffsetPaginator[ledger.Account, any]{
		DefaultPaginationColumn: "address",
		DefaultOrder:            bunpaginate.OrderAsc,
	})
}

func (store *Store) BeginTX(ctx context.Context, options *sql.TxOptions) (*Store, *bun.Tx, error) {
	tx, err := store.db.BeginTx(ctx, options)
	if err != nil {
		return nil, nil, postgres.ResolveError(err)
	}
	cp := *store
	cp.db = tx

	return &cp, &tx, nil
}

func (store *Store) Commit() error {
	switch db := store.db.(type) {
	case bun.Tx:
		return db.Commit()
	default:
		return errors.New("cannot commit transaction: not in a transaction")
	}
}

func (store *Store) Rollback() error {
	switch db := store.db.(type) {
	case bun.Tx:
		return db.Rollback()
	default:
		return errors.New("cannot rollback transaction: not in a transaction")
	}
}

func (store *Store) GetLedger() ledger.Ledger {
	return store.ledger
}

func (store *Store) GetDB() bun.IDB {
	return store.db
}

func (store *Store) GetBucket() bucket.Bucket {
	return store.bucket
}

func (store *Store) GetPrefixedRelationName(v string) string {
	return fmt.Sprintf(`"%s".%s`, store.ledger.Bucket, v)
}

func validateAddressFilter(ledger ledger.Ledger, operator string, value any) error {
	if operator != "$match" {
		return fmt.Errorf("'address' column can only be used with $match, operator used is: %s", operator)
	}
	if value, ok := value.(string); !ok {
		return fmt.Errorf("invalid 'address' filter")
	} else if isSegmentedAddress(value) && !ledger.HasFeature(features.FeatureIndexAddressSegments, "ON") {
		return fmt.Errorf("feature %s must be 'ON' to use segments address", features.FeatureIndexAddressSegments)
	}

	return nil
}

func (store *Store) LockLedger(ctx context.Context) error {
	_, err := store.db.NewRaw(`lock table ` + store.GetPrefixedRelationName("logs")).Exec(ctx)
	return postgres.ResolveError(err)
}

func New(db bun.IDB, bucket bucket.Bucket, l ledger.Ledger, opts ...Option) *Store {
	ret := &Store{
		db:     db,
		ledger: l,
		bucket: bucket,
	}
	for _, opt := range append(defaultOptions, opts...) {
		opt(ret)
	}

	var err error
	ret.checkBucketSchemaHistogram, err = ret.meter.Int64Histogram("store.checkBucketSchema")
	if err != nil {
		panic(err)
	}

	ret.checkLedgerSchemaHistogram, err = ret.meter.Int64Histogram("store.checkLedgerSchema")
	if err != nil {
		panic(err)
	}

	ret.updateAccountsMetadataHistogram, err = ret.meter.Int64Histogram("store.updateAccountsMetadata")
	if err != nil {
		panic(err)
	}

	ret.deleteAccountMetadataHistogram, err = ret.meter.Int64Histogram("store.deleteAccountMetadata")
	if err != nil {
		panic(err)
	}

	ret.upsertAccountsHistogram, err = ret.meter.Int64Histogram("store.upsertAccounts")
	if err != nil {
		panic(err)
	}

	ret.getBalancesHistogram, err = ret.meter.Int64Histogram("store.getBalances")
	if err != nil {
		panic(err)
	}

	ret.insertLogHistogram, err = ret.meter.Int64Histogram("store.insertLog")
	if err != nil {
		panic(err)
	}

	ret.readLogWithIdempotencyKeyHistogram, err = ret.meter.Int64Histogram("store.readLogWithIdempotencyKey")
	if err != nil {
		panic(err)
	}

	ret.insertMovesHistogram, err = ret.meter.Int64Histogram("store.insertMoves")
	if err != nil {
		panic(err)
	}

	ret.insertTransactionHistogram, err = ret.meter.Int64Histogram("store.insertTransaction")
	if err != nil {
		panic(err)
	}

	ret.revertTransactionHistogram, err = ret.meter.Int64Histogram("store.revertTransaction")
	if err != nil {
		panic(err)
	}

	ret.updateTransactionMetadataHistogram, err = ret.meter.Int64Histogram("store.updateTransactionMetadata")
	if err != nil {
		panic(err)
	}

	ret.deleteTransactionMetadataHistogram, err = ret.meter.Int64Histogram("store.deleteTransactionMetadata")
	if err != nil {
		panic(err)
	}

	ret.updateBalancesHistogram, err = ret.meter.Int64Histogram("store.updateBalances")
	if err != nil {
		panic(err)
	}

	ret.getVolumesWithBalancesHistogram, err = ret.meter.Int64Histogram("store.getVolumesWithBalances")
	if err != nil {
		panic(err)
	}

	return ret
}

func (store *Store) HasMinimalVersion(ctx context.Context) (bool, error) {
	return store.bucket.HasMinimalVersion(ctx)
}

func (store *Store) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return store.bucket.GetMigrationsInfo(ctx)
}

func (store *Store) WithDB(db bun.IDB) *Store {
	ret := *store
	ret.db = db

	return &ret
}

type Option func(s *Store)

func WithMeter(meter metric.Meter) Option {
	return func(s *Store) {
		s.meter = meter
	}
}

func WithTracer(tracer trace.Tracer) Option {
	return func(s *Store) {
		s.tracer = tracer
	}
}

var defaultOptions = []Option{
	WithMeter(noopmetrics.Meter{}),
	WithTracer(nooptracer.Tracer{}),
}
