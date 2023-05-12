package system

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
	storageerrors "github.com/formancehq/ledger/pkg/storage/sqlstorage/errors"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

const ledgersTableName = "ledgers"

type Ledgers struct {
	bun.BaseModel `bun:"ledgers,alias:ledgers"`

	Ledger  string    `bun:"ledger,type:varchar(255),pk"` // Primary key
	AddedAt core.Time `bun:"addedAt,type:timestamp"`
}

func (s *Store) CreateLedgersTable(ctx context.Context) error {
	_, err := s.schema.NewCreateTable(ledgersTableName).
		Model((*Ledgers)(nil)).
		IfNotExists().
		Exec(ctx)

	return storageerrors.PostgresError(err)
}

func (s *Store) ListLedgers(ctx context.Context) ([]string, error) {
	query := s.schema.NewSelect(ledgersTableName).
		Model((*Ledgers)(nil)).
		Column("ledger").
		String()

	rows, err := s.schema.QueryContext(ctx, query)
	if err != nil {
		return nil, storageerrors.PostgresError(err)
	}
	defer rows.Close()

	res := make([]string, 0)
	for rows.Next() {
		var ledger string
		if err := rows.Scan(&ledger); err != nil {
			return nil, storageerrors.PostgresError(err)
		}
		res = append(res, ledger)
	}
	return res, nil
}

func (s *Store) DeleteLedger(ctx context.Context, name string) error {
	_, err := s.schema.NewDelete(ledgersTableName).
		Model((*Ledgers)(nil)).
		Where("ledger = ?", name).
		Exec(ctx)

	return errors.Wrap(storageerrors.PostgresError(err), "delete ledger from system store")
}

func (s *Store) Register(ctx context.Context, ledger string) (bool, error) {
	l := &Ledgers{
		Ledger:  ledger,
		AddedAt: core.Now(),
	}

	ret, err := s.schema.NewInsert(ledgersTableName).
		Model(l).
		Ignore().
		Exec(ctx)
	if err != nil {
		return false, storageerrors.PostgresError(err)
	}

	affected, err := ret.RowsAffected()
	if err != nil {
		return false, storageerrors.PostgresError(err)
	}

	return affected > 0, nil
}

func (s *Store) Exists(ctx context.Context, ledger string) (bool, error) {
	query := s.schema.NewSelect(ledgersTableName).
		Model((*Ledgers)(nil)).
		Column("ledger").
		Where("ledger = ?", ledger).
		String()

	ret := s.schema.QueryRowContext(ctx, query)
	if ret.Err() != nil {
		return false, nil
	}

	var t string
	_ = ret.Scan(&t) // Trigger close

	if t == "" {
		return false, nil
	}
	return true, nil
}
