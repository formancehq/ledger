package system

import (
	"context"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/uptrace/bun"
)

type Store interface {
	CreateLedger(ctx context.Context, l *ledger.Ledger) error
	DeleteLedgerMetadata(ctx context.Context, name string, key string) error
	UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error
	ListLedgers(ctx context.Context, q ledgercontroller.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error)
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
	db bun.IDB
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

func (d *DefaultStore) ListLedgers(ctx context.Context, q ledgercontroller.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error) {
	query := d.db.NewSelect().
		Model(&ledger.Ledger{}).
		Column("*").
		Order("added_at asc")

	if len(q.Options.Options.Features) > 0 {
		for key, value := range q.Options.Options.Features {
			query = query.Where("features->>? = ?", key, value)
		}
	}

	if q.Options.Options.Bucket != "" {
		query = query.Where("bucket = ?", q.Options.Options.Bucket)
	}

	return bunpaginate.UsingOffset[ledgercontroller.PaginatedQueryOptions[ledgercontroller.ListLedgersQueryPayload], ledger.Ledger](
		ctx,
		query,
		bunpaginate.OffsetPaginatedQuery[ledgercontroller.PaginatedQueryOptions[ledgercontroller.ListLedgersQueryPayload]](q),
	)
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
	return d.GetMigrator(options...).Up(ctx)
}

func (d *DefaultStore) GetMigrator(options ...migrations.Option) *migrations.Migrator {
	return GetMigrator(d.db, options...)
}

func New(db bun.IDB) *DefaultStore {
	return &DefaultStore{
		db: db,
	}
}
