package ledger

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/platform/postgres"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/internal/tracing"

	"errors"
	"github.com/formancehq/go-libs/migrations"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/uptrace/bun"
)

type Store struct {
	db     bun.IDB
	ledger ledger.Ledger

	meter                              metric.Meter
	listAccountsHistogram              metric.Int64Histogram
	checkBucketSchemaHistogram         metric.Int64Histogram
	checkLedgerSchemaHistogram         metric.Int64Histogram
	getAccountHistogram                metric.Int64Histogram
	countAccountsHistogram             metric.Int64Histogram
	updateAccountsMetadataHistogram    metric.Int64Histogram
	deleteAccountMetadataHistogram     metric.Int64Histogram
	upsertAccountHistogram             metric.Int64Histogram
	getBalancesHistogram               metric.Int64Histogram
	insertLogHistogram                 metric.Int64Histogram
	listLogsHistogram                  metric.Int64Histogram
	readLogWithIdempotencyKeyHistogram metric.Int64Histogram
	insertMovesHistogram               metric.Int64Histogram
	countTransactionsHistogram         metric.Int64Histogram
	getTransactionHistogram            metric.Int64Histogram
	insertTransactionHistogram         metric.Int64Histogram
	revertTransactionHistogram         metric.Int64Histogram
	updateTransactionMetadataHistogram metric.Int64Histogram
	deleteTransactionMetadataHistogram metric.Int64Histogram
	updateBalancesHistogram            metric.Int64Histogram
	getVolumesWithBalancesHistogram    metric.Int64Histogram
	listTransactionsHistogram          metric.Int64Histogram
}

func (s *Store) GetDB() bun.IDB {
	return s.db
}

func (s *Store) GetPrefixedRelationName(v string) string {
	return fmt.Sprintf(`"%s".%s`, s.ledger.Bucket, v)
}

func (s *Store) WithDB(db bun.IDB) *Store {
	ret := *s
	ret.db = db
	return &ret
}

// todo: merge with bucket migration info
// todo: add test
func (s *Store) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return getMigrator(s.ledger).GetMigrations(ctx, s.db)
}

func (s *Store) IsUpToDate(ctx context.Context) (bool, error) {
	bucketUpToDate, err := tracing.TraceWithMetric(ctx, "CheckBucketSchema", s.checkBucketSchemaHistogram, func(ctx context.Context) (bool, error) {
		return bucket.New(s.db, s.ledger.Bucket).IsUpToDate(ctx)
	})
	if err != nil {
		return false, fmt.Errorf("failed to check if bucket is up to date: %w", err)
	}
	if !bucketUpToDate {
		return false, nil
	}

	ret, err := tracing.TraceWithMetric(ctx, "CheckLedgerSchema", s.checkLedgerSchemaHistogram, func(ctx context.Context) (bool, error) {
		return getMigrator(s.ledger).IsUpToDate(ctx, s.db)
	})
	if err != nil && errors.Is(err, migrations.ErrMissingVersionTable) {
		return false, nil
	}

	return ret, err
}

func (s *Store) validateAddressFilter(operator string, value any) error {
	if operator != "$match" {
		return errors.New("'address' column can only be used with $match")
	}
	if value, ok := value.(string); !ok {
		return fmt.Errorf("invalid 'address' filter")
	} else if isSegmentedAddress(value) && !s.ledger.HasFeature(ledger.FeatureIndexAddressSegments, "ON") {
		return fmt.Errorf("feature %s must be 'ON' to use segments address", ledger.FeatureIndexAddressSegments)
	}

	return nil
}

func (s *Store) LockLedger(ctx context.Context) error {
	_, err := s.db.NewRaw(`lock table ` + s.GetPrefixedRelationName("logs")).Exec(ctx)
	return postgres.ResolveError(err)
}

func New(db bun.IDB, ledger ledger.Ledger, opts ...Option) *Store {
	ret := &Store{
		db:     db,
		ledger: ledger,
	}
	for _, opt := range append(defaultOptions, opts...) {
		opt(ret)
	}

	var err error
	ret.listAccountsHistogram, err = ret.meter.Int64Histogram("store.listAccounts")
	if err != nil {
		panic(err)
	}
	ret.checkBucketSchemaHistogram, err = ret.meter.Int64Histogram("store.checkBucketSchema")
	if err != nil {
		panic(err)
	}
	ret.checkLedgerSchemaHistogram, err = ret.meter.Int64Histogram("store.checkLedgerSchema")
	if err != nil {
		panic(err)
	}
	ret.getAccountHistogram, err = ret.meter.Int64Histogram("store.getAccount")
	if err != nil {
		panic(err)
	}
	ret.countAccountsHistogram, err = ret.meter.Int64Histogram("store.countAccounts")
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
	ret.upsertAccountHistogram, err = ret.meter.Int64Histogram("store.upsertAccount")
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
	ret.listLogsHistogram, err = ret.meter.Int64Histogram("store.listLogs")
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
	ret.countTransactionsHistogram, err = ret.meter.Int64Histogram("store.countTransactions")
	if err != nil {
		panic(err)
	}
	ret.getTransactionHistogram, err = ret.meter.Int64Histogram("store.getTransaction")
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
	ret.listTransactionsHistogram, err = ret.meter.Int64Histogram("store.listTransactions")
	if err != nil {
		panic(err)
	}

	return ret
}

type Option func(s *Store)

func WithMeter(meter metric.Meter) Option {
	return func(s *Store) {
		s.meter = meter
	}
}

var defaultOptions = []Option{
	WithMeter(noop.Meter{}),
}
