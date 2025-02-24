package system

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/lib/pq"
	"github.com/uptrace/bun"
)

const (
	SchemaSystem = "_system"
)

var (
	ErrLedgerAlreadyExists = errors.New("ledger already exists")
)

type DefaultStore struct {
	db *bun.DB
}

func (store *DefaultStore) IsUpToDate(ctx context.Context) (bool, error) {
	return store.GetMigrator().IsUpToDate(ctx)
}

func (store *DefaultStore) GetDistinctBuckets(ctx context.Context) ([]string, error) {
	var buckets []string
	err := store.db.NewSelect().
		DistinctOn("bucket").
		Model(&ledger.Ledger{}).
		Column("bucket").
		Scan(ctx, &buckets)
	if err != nil {
		return nil, fmt.Errorf("getting buckets: %w", postgres.ResolveError(err))
	}

	return buckets, nil
}

func (store *DefaultStore) CreateLedger(ctx context.Context, l *ledger.Ledger) error {

	if l.Metadata == nil {
		l.Metadata = metadata.Metadata{}
	}

	_, err := store.db.NewInsert().
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

func (store *DefaultStore) UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error {
	_, err := store.db.NewUpdate().
		Model(&ledger.Ledger{}).
		Set("metadata = metadata || ?", m).
		Where("name = ?", name).
		Exec(ctx)
	return err
}

func (store *DefaultStore) DeleteLedgerMetadata(ctx context.Context, name string, key string) error {
	_, err := store.db.NewUpdate().
		Model(&ledger.Ledger{}).
		Set("metadata = metadata - ?", key).
		Where("name = ?", name).
		Exec(ctx)
	return err
}

func (store *DefaultStore) ListLedgers(ctx context.Context, q ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error) {
	query := store.db.NewSelect().
		Model(&ledger.Ledger{}).
		Column("*").
		Order("added_at asc")

	return bunpaginate.UsingOffset[ledgerstore.PaginatedQueryOptions[struct{}], ledger.Ledger](
		ctx,
		query,
		bunpaginate.OffsetPaginatedQuery[ledgerstore.PaginatedQueryOptions[struct{}]](q),
	)
}

func (store *DefaultStore) GetLedger(ctx context.Context, name string) (*ledger.Ledger, error) {
	ret := &ledger.Ledger{}
	if err := store.db.NewSelect().
		Model(ret).
		Column("*").
		Where("name = ?", name).
		Scan(ctx); err != nil {
		return nil, postgres.ResolveError(err)
	}

	return ret, nil
}

func (store *DefaultStore) Migrate(ctx context.Context, options ...migrations.Option) error {
	return store.GetMigrator(options...).Up(ctx)
}

func (store *DefaultStore) GetMigrator(options ...migrations.Option) *migrations.Migrator {
	return GetMigrator(store.db, options...)
}

func (store *DefaultStore) GetDB() *bun.DB {
	return store.db
}

func New(db *bun.DB) *DefaultStore {
	return &DefaultStore{
		db: db,
	}
}

func (store *DefaultStore) ListConnectors(ctx context.Context) (*bunpaginate.Cursor[ledger.Connector], error) {
	return bunpaginate.UsingOffset[struct{}, ledger.Connector](
		ctx,
		store.db.NewSelect(),
		bunpaginate.OffsetPaginatedQuery[struct{}]{},
	)
}

func (store *DefaultStore) CreateConnector(ctx context.Context, connector ledger.Connector) error {
	_, err := store.db.NewInsert().
		Model(&connector).
		Exec(ctx)
	return err
}

func (store *DefaultStore) DeleteConnector(ctx context.Context, id string) error {
	ret, err := store.db.NewDelete().
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

func (store *DefaultStore) GetConnector(ctx context.Context, id string) (*ledger.Connector, error) {
	ret := &ledger.Connector{}
	err := store.db.NewSelect().
		Model(ret).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (store *DefaultStore) ListPipelines(ctx context.Context) (*bunpaginate.Cursor[ledger.Pipeline], error) {
	return bunpaginate.UsingOffset[struct{}, ledger.Pipeline](
		ctx,
		store.db.NewSelect(),
		bunpaginate.OffsetPaginatedQuery[struct{}]{},
	)
}

func (store *DefaultStore) CreatePipeline(ctx context.Context, pipeline ledger.Pipeline) error {
	_, err := store.db.NewInsert().
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

func (store *DefaultStore) UpdatePipeline(ctx context.Context, id string, o map[string]any) error {
	updateQuery := store.db.NewUpdate().
		Table("_system.pipelines")
	for k, v := range o {
		updateQuery = updateQuery.Set(k + " = ?", v)
	}
	updateQuery = updateQuery.
		Set("version = version + 1").
		Where("id = ?", id)

	_, err := updateQuery.Exec(ctx)
	return postgres.ResolveError(err)
}

func (store *DefaultStore) DeletePipeline(ctx context.Context, id string) error {
	ret, err := store.db.NewDelete().
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

func (store *DefaultStore) GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error) {
	ret := &ledger.Pipeline{}
	err := store.db.NewSelect().
		Model(ret).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (store *DefaultStore) ListEnabledPipelines(ctx context.Context) ([]ledger.Pipeline, error) {
	ret := make([]ledger.Pipeline, 0)
	if err := store.db.NewSelect().
		Model(&ret).
		Where("enabled").
		Scan(ctx); err != nil {
		return nil, err
	}
	return ret, nil
}

func (store *DefaultStore) StorePipelineState(ctx context.Context, id string, lastLogID int) error {
	ret, err := store.db.NewUpdate().
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
