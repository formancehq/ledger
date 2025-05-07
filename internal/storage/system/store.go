package system

import (
	"context"
	"errors"
	"fmt"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/migrations"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	gotime "github.com/formancehq/go-libs/v3/time"
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
	MarkBucketAsDeleted(ctx context.Context, bucketName string) error
	RestoreBucket(ctx context.Context, bucketName string) error
	ListBucketsWithStatus(ctx context.Context, query common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[systemcontroller.BucketWithStatus], error)
	ListBuckets(ctx context.Context, query common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Bucket], error)

	Migrate(ctx context.Context, options ...migrations.Option) error
	GetMigrator(options ...migrations.Option) *migrations.Migrator
	IsUpToDate(ctx context.Context) (bool, error)
	CreateBucket(ctx context.Context, b *ledger.Bucket) error
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
	var bucketNames []string
	err := d.db.NewSelect().
		Model(&ledger.Bucket{}).
		Where("deleted_at IS NULL").
		Column("name").
		Scan(ctx, &bucketNames)
	if err != nil {
		return nil, fmt.Errorf("getting buckets: %w", postgres.ResolveError(err))
	}

	return bucketNames, nil
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
	err := d.db.NewSelect().
		Model(ret).
		Column("_system.ledgers.*").
		Join("LEFT JOIN _system.buckets ON _system.ledgers.bucket = _system.buckets.name").
		Where("_system.ledgers.name = ?", name).
		Where("_system.buckets.deleted_at IS NULL").
		Scan(ctx)

	if err != nil {
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
		Model(&ledger.Bucket{}).
		Set("deleted_at = now()").
		Where("name = ?", bucketName).
		Exec(ctx)
	return postgres.ResolveError(err)
}

func (d *DefaultStore) RestoreBucket(ctx context.Context, bucketName string) error {
	_, err := d.db.NewUpdate().
		Model(&ledger.Bucket{}).
		Set("deleted_at = NULL").
		Where("name = ?", bucketName).
		Exec(ctx)
	return postgres.ResolveError(err)
}

func (d *DefaultStore) ListBuckets(ctx context.Context, query common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Bucket], error) {
	var buckets []ledger.Bucket

	q := d.db.NewSelect().
		Model(&ledger.Bucket{}).
		Column("name", "added_at", "deleted_at")

	if query.PageSize == 0 {
		query.PageSize = bunpaginate.QueryDefaultPageSize
	}

	if query.Column == "" {
		query.Column = "name"
	}

	paginator := common.ColumnPaginator[ledger.Bucket, any]{
		DefaultPaginationColumn: "name",
		DefaultOrder:            bunpaginate.OrderAsc,
	}

	var err error
	q, err = paginator.Paginate(q, query)
	if err != nil {
		return nil, fmt.Errorf("applying pagination: %w", err)
	}

	err = q.Scan(ctx, &buckets)
	if err != nil {
		return nil, fmt.Errorf("getting buckets: %w", postgres.ResolveError(err))
	}

	cursor, err := paginator.BuildCursor(buckets, query)
	if err != nil {
		return nil, fmt.Errorf("building cursor: %w", err)
	}

	return cursor, nil
}

func (d *DefaultStore) ListBucketsWithStatus(ctx context.Context, query common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[systemcontroller.BucketWithStatus], error) {
	bucketsCursor, err := d.ListBuckets(ctx, query)
	if err != nil {
		return nil, err
	}

	// Map Bucket to BucketWithStatus
	bucketsWithStatus := make([]systemcontroller.BucketWithStatus, len(bucketsCursor.Data))
	for i, bucket := range bucketsCursor.Data {

		bucketsWithStatus[i] = systemcontroller.BucketWithStatus{
			Name:      bucket.Name,
			DeletedAt: bucket.DeletedAt,
		}
	}

	return &bunpaginate.Cursor[systemcontroller.BucketWithStatus]{
		Data:     bucketsWithStatus,
		PageSize: bucketsCursor.PageSize,
		HasMore:  bucketsCursor.HasMore,
		Next:     bucketsCursor.Next,
		Previous: bucketsCursor.Previous,
	}, nil
}

func (d *DefaultStore) CreateBucket(ctx context.Context, b *ledger.Bucket) error {
	if b.AddedAt.IsZero() {
		b.AddedAt = gotime.Now()
	}

	_, err := d.db.NewInsert().
		Model(b).
		Returning("id, added_at").
		Exec(ctx)
	if err != nil {
		if errors.Is(postgres.ResolveError(err), postgres.ErrConstraintsFailed{}) {
			return fmt.Errorf("bucket already exists: %s", b.Name)
		}
		return postgres.ResolveError(err)
	}

	return nil
}

var defaultOptions = []Option{
	WithTracer(noop.Tracer{}),
}
