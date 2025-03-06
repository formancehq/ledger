package system

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/migrations"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/internal/tracing"
	"github.com/lib/pq"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type Store interface {
	CreateLedger(ctx context.Context, l *ledger.Ledger) error
	DeleteLedgerMetadata(ctx context.Context, name string, key string) error
	UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error
	Ledgers() common.PaginatedResource[ledger.Ledger, ListLedgersQueryPayload]
	GetLedger(ctx context.Context, name string) (*ledger.Ledger, error)
	GetDistinctBuckets(ctx context.Context) ([]string, error)

	Migrate(ctx context.Context, options ...migrations.Option) error
	GetMigrator(options ...migrations.Option) *migrations.Migrator
	IsUpToDate(ctx context.Context) (bool, error)
}

const (
	SchemaSystem = "_system"
)

var (
	ErrLedgerAlreadyExists = errors.New("ledger already exists")
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
			return ErrLedgerAlreadyExists
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
	ListLedgersQueryPayload] {
	return common.NewPaginatedResourceRepository[ledger.Ledger, ListLedgersQueryPayload](&ledgersResourceHandler{store: d}, "id", bunpaginate.OrderAsc)
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

var defaultOptions = []Option{
	WithTracer(noop.Tracer{}),
}

func (d *DefaultStore) ListConnectors(ctx context.Context) (*bunpaginate.Cursor[ledger.Connector], error) {
	return bunpaginate.UsingOffset[struct{}, ledger.Connector](
		ctx,
		d.db.NewSelect(),
		bunpaginate.OffsetPaginatedQuery[struct{}]{},
	)
}

func (d *DefaultStore) CreateConnector(ctx context.Context, connector ledger.Connector) error {
	_, err := d.db.NewInsert().
		Model(&connector).
		Exec(ctx)
	return err
}

func (d *DefaultStore) DeleteConnector(ctx context.Context, id string) error {
	ret, err := d.db.NewDelete().
		Model(&ledger.Connector{}).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		switch err := err.(type) {
		case *pq.Error:
			if err.Constraint == "pipelines_connector_id_fkey" {
				return ledger.NewErrConnectorUsed(id)
			}
			return err
		default:
			return err
		}
	}

	rowsAffected, err := ret.RowsAffected()
	if err != nil {
		panic(err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return err
}

func (d *DefaultStore) GetConnector(ctx context.Context, id string) (*ledger.Connector, error) {
	ret := &ledger.Connector{}
	err := d.db.NewSelect().
		Model(ret).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (d *DefaultStore) ListPipelines(ctx context.Context) (*bunpaginate.Cursor[ledger.Pipeline], error) {
	return bunpaginate.UsingOffset[struct{}, ledger.Pipeline](
		ctx,
		d.db.NewSelect(),
		bunpaginate.OffsetPaginatedQuery[struct{}]{},
	)
}

func (d *DefaultStore) CreatePipeline(ctx context.Context, pipeline ledger.Pipeline) error {
	_, err := d.db.NewInsert().
		Model(&pipeline).
		Exec(ctx)
	if err != nil {
		// notes(gfyrag): it is not safe to check errors like that
		// but *pq.Error does not implement standard go utils for errors
		// so, we don't have choice
		err := postgres.ResolveError(err)
		if errors.Is(err, postgres.ErrConstraintsFailed{}) {
			return ledger.NewErrPipelineAlreadyExists(pipeline.PipelineConfiguration)
		}

		return err
	}
	return nil
}

func (d *DefaultStore) UpdatePipeline(ctx context.Context, id string, o map[string]any) error {
	updateQuery := d.db.NewUpdate().
		Table("_system.pipelines")
	for k, v := range o {
		updateQuery = updateQuery.Set(k+" = ?", v)
	}
	updateQuery = updateQuery.
		Set("version = version + 1").
		Where("id = ?", id)

	_, err := updateQuery.Exec(ctx)
	return postgres.ResolveError(err)
}

func (d *DefaultStore) DeletePipeline(ctx context.Context, id string) error {
	ret, err := d.db.NewDelete().
		Model(&ledger.Pipeline{}).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return err
	}

	rowsAffected, err := ret.RowsAffected()
	if err != nil {
		panic(err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return err
}

func (d *DefaultStore) GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error) {
	ret := &ledger.Pipeline{}
	err := d.db.NewSelect().
		Model(ret).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (d *DefaultStore) ListEnabledPipelines(ctx context.Context) ([]ledger.Pipeline, error) {
	ret := make([]ledger.Pipeline, 0)
	if err := d.db.NewSelect().
		Model(&ret).
		Where("enabled").
		Scan(ctx); err != nil {
		return nil, err
	}
	return ret, nil
}

func (d *DefaultStore) StorePipelineState(ctx context.Context, id string, lastLogID int) error {
	ret, err := d.db.NewUpdate().
		Model(&ledger.Pipeline{}).
		Where("id = ?", id).
		Set("last_log_id = ?", lastLogID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("updating state in database: %w", err)
	}
	rowsAffected, err := ret.RowsAffected()
	if err != nil {
		panic(err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}
