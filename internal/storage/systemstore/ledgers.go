package systemstore

import (
	"context"

	storageerrors "github.com/formancehq/ledger/internal/storage/sqlutils"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

type Ledgers struct {
	bun.BaseModel `bun:"_system.ledgers,alias:ledgers"`

	Ledger  string      `bun:"ledger,type:varchar(255),pk"` // Primary key
	AddedAt ledger.Time `bun:"addedat,type:timestamp"`
}

func (s *Store) ListLedgers(ctx context.Context) ([]string, error) {
	query := s.db.NewSelect().
		Model((*Ledgers)(nil)).
		Column("ledger").
		String()

	rows, err := s.db.QueryContext(ctx, query)
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
	_, err := s.db.NewDelete().
		Model((*Ledgers)(nil)).
		Where("ledger = ?", name).
		Exec(ctx)

	return errors.Wrap(storageerrors.PostgresError(err), "delete ledger from system store")
}

func (s *Store) Register(ctx context.Context, ledgerName string) (bool, error) {
	l := &Ledgers{
		Ledger:  ledgerName,
		AddedAt: ledger.Now(),
	}

	ret, err := s.db.NewInsert().
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
	query := s.db.NewSelect().
		Model((*Ledgers)(nil)).
		Column("ledger").
		Where("ledger = ?", ledger).
		String()

	ret := s.db.QueryRowContext(ctx, query)
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
