package systemstore

import (
	"context"

	"github.com/formancehq/stack/libs/go-libs/time"

	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"

	"github.com/formancehq/ledger/internal/storage/sqlutils"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

type Ledger struct {
	bun.BaseModel `bun:"_system.ledgers,alias:ledgers"`

	Name    string    `bun:"ledger,type:varchar(255),pk" json:"name"` // Primary key
	AddedAt time.Time `bun:"addedat,type:timestamp" json:"addedAt"`
	Bucket  string    `bun:"bucket,type:varchar(255)" json:"bucket"`
}

type PaginatedQueryOptions struct {
	PageSize uint64 `json:"pageSize"`
}

type ListLedgersQuery bunpaginate.OffsetPaginatedQuery[PaginatedQueryOptions]

func (query ListLedgersQuery) WithPageSize(pageSize uint64) ListLedgersQuery {
	query.PageSize = pageSize
	return query
}

func NewListLedgersQuery(pageSize uint64) ListLedgersQuery {
	return ListLedgersQuery{
		PageSize: pageSize,
	}
}

func (s *Store) ListLedgers(ctx context.Context, q ListLedgersQuery) (*bunpaginate.Cursor[Ledger], error) {
	query := s.db.NewSelect().
		Column("ledger", "bucket", "addedat").
		Order("addedat asc")

	return bunpaginate.UsingOffset[PaginatedQueryOptions, Ledger](ctx, query, bunpaginate.OffsetPaginatedQuery[PaginatedQueryOptions](q))
}

func (s *Store) DeleteLedger(ctx context.Context, name string) error {
	_, err := s.db.NewDelete().
		Model((*Ledger)(nil)).
		Where("ledger = ?", name).
		Exec(ctx)

	return errors.Wrap(sqlutils.PostgresError(err), "delete ledger from system store")
}

func (s *Store) RegisterLedger(ctx context.Context, l *Ledger) (bool, error) {
	return RegisterLedger(ctx, s.db, l)
}

func (s *Store) GetLedger(ctx context.Context, name string) (*Ledger, error) {
	ret := &Ledger{}
	if err := s.db.NewSelect().
		Model(ret).
		Column("ledger", "bucket", "addedat").
		Where("ledger = ?", name).
		Scan(ctx); err != nil {
		return nil, sqlutils.PostgresError(err)
	}

	return ret, nil
}

func RegisterLedger(ctx context.Context, db bun.IDB, l *Ledger) (bool, error) {
	ret, err := db.NewInsert().
		Model(l).
		Ignore().
		Exec(ctx)
	if err != nil {
		return false, sqlutils.PostgresError(err)
	}

	affected, err := ret.RowsAffected()
	if err != nil {
		return false, sqlutils.PostgresError(err)
	}

	return affected > 0, nil
}
