package system

import (
	"context"
	"errors"
	"fmt"
	stdtime "time"
	
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/migrations"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger/internal"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/internal/tracing"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type Store interface {
	CreateLedger(ctx context.Context, l *ledger.Ledger) error
	DeleteLedgerMetadata(ctx context.Context, name string, key string) error
	UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error
	Ledgers() common.PaginatedResource[ledger.Ledger, any, common.ColumnPaginatedQuery[any]]
	GetLedger(ctx context.Context, name string) (*ledger.Ledger, error)
	GetDistinctBuckets(ctx context.Context) ([]string, error)

	Migrate(ctx context.Context, options ...migrations.Option) error
	GetMigrator(options ...migrations.Option) *migrations.Migrator
	IsUpToDate(ctx context.Context) (bool, error)
}

const (
	SchemaSystem = "_system"
)

type DefaultStore struct {
	db     bun.IDB
	tracer trace.Tracer
}

func (d *DefaultStore) IsUpToDate(ctx context.Context) (bool, error) {
	return d.GetMigrator().IsUpToDate(ctx)
}

func (d *DefaultStore) GetDistinctBuckets(ctx context.Context) ([]string, error) {
	var buckets []string
	err := d.db.NewSelect().
		DistinctOn("bucket").
		Model(&ledger.Ledger{}).
		Column("bucket").
		Where("deleted_at IS NULL").
		Scan(ctx, &buckets)
	if err != nil {
		return nil, fmt.Errorf("getting buckets: %w", postgres.ResolveError(err))
	}

	return buckets, nil
}

func (d *DefaultStore) CreateLedger(ctx context.Context, l *ledger.Ledger) error {

	if l.Metadata == nil {
		l.Metadata = metadata.Metadata{}
	}

	_, err := d.db.NewInsert().
		Model(l).
		Returning("id, added_at").
		Exec(ctx)
	if err != nil {
		if errors.Is(postgres.ResolveError(err), postgres.ErrConstraintsFailed{}) {
			return systemcontroller.ErrLedgerAlreadyExists
		}
		return postgres.ResolveError(err)
	}

	return nil
}

func (d *DefaultStore) UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error {
	_, err := d.db.NewUpdate().
		Model(&ledger.Ledger{}).
		Set("metadata = metadata || ?", m).
		Where("name = ?", name).
		Exec(ctx)
	return err
}

func (d *DefaultStore) DeleteLedgerMetadata(ctx context.Context, name string, key string) error {
	_, err := d.db.NewUpdate().
		Model(&ledger.Ledger{}).
		Set("metadata = metadata - ?", key).
		Where("name = ?", name).
		Exec(ctx)
	return err
}

func (d *DefaultStore) Ledgers() common.PaginatedResource[
	ledger.Ledger,
	any,
	common.ColumnPaginatedQuery[any]] {
	return common.NewPaginatedResourceRepository(&ledgersResourceHandler{store: d}, common.ColumnPaginator[ledger.Ledger, any]{
		DefaultPaginationColumn: "id",
		DefaultOrder:            bunpaginate.OrderAsc,
	})
}

func (d *DefaultStore) GetLedger(ctx context.Context, name string) (*ledger.Ledger, error) {
	ret := &ledger.Ledger{}
	if err := d.db.NewSelect().
		Model(ret).
		Column("*").
		Where("name = ?", name).
		Scan(ctx); err != nil {
		return nil, postgres.ResolveError(err)
	}

	return ret, nil
}

func (d *DefaultStore) Migrate(ctx context.Context, options ...migrations.Option) error {
	_, err := tracing.Trace(ctx, d.tracer, "MigrateSystemStore", func(ctx context.Context) (any, error) {
		return nil, d.GetMigrator(options...).Up(ctx)
	})
	return err

}

func (d *DefaultStore) GetMigrator(options ...migrations.Option) *migrations.Migrator {
	return GetMigrator(d.db, append(options, migrations.WithTracer(d.tracer))...)
}

func New(db bun.IDB, opts ...Option) *DefaultStore {
	ret := &DefaultStore{
		db: db,
	}

	for _, opt := range append(defaultOptions, opts...) {
		opt(ret)
	}

	return ret
}

type Option func(*DefaultStore)

func WithTracer(tracer trace.Tracer) Option {
	return func(d *DefaultStore) {
		d.tracer = tracer
	}
}

func (d *DefaultStore) MarkBucketAsDeleted(ctx context.Context, bucketName string) error {
	_, err := d.db.NewUpdate().
		Model(&ledger.Ledger{}).
		Set("deleted_at = ?", stdtime.Now().UTC()).
		Where("bucket = ?", bucketName).
		Exec(ctx)
	return postgres.ResolveError(err)
}

func (d *DefaultStore) RestoreBucket(ctx context.Context, bucketName string) error {
	_, err := d.db.NewUpdate().
		Model(&ledger.Ledger{}).
		Set("deleted_at = NULL").
		Where("bucket = ?", bucketName).
		Exec(ctx)
	return postgres.ResolveError(err)
}

func (d *DefaultStore) ListBucketsWithStatus(ctx context.Context) ([]systemcontroller.BucketWithStatus, error) {
	var results []struct {
		Bucket    string       `bun:"bucket"`
		DeletedAt stdtime.Time `bun:"deleted_at"`
	}

	err := d.db.NewSelect().
		DistinctOn("bucket").
		Model(&ledger.Ledger{}).
		Column("bucket", "deleted_at").
		Scan(ctx, &results)
	if err != nil {
		return nil, fmt.Errorf("getting buckets with status: %w", postgres.ResolveError(err))
	}

	buckets := make([]systemcontroller.BucketWithStatus, len(results))
	for i, result := range results {
		var deletedAt time.Time
		if !result.DeletedAt.IsZero() {
			deletedAt = time.Time{Time: result.DeletedAt}
		}
		buckets[i] = systemcontroller.BucketWithStatus{
			Name:      result.Bucket,
			DeletedAt: deletedAt,
		}
	}

	return buckets, nil
}

var defaultOptions = []Option{
	WithTracer(noop.Tracer{}),
}
