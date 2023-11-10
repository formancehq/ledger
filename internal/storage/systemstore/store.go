package systemstore

import (
	"context"

	"github.com/formancehq/ledger/internal/storage/sqlutils"

	"github.com/uptrace/bun"
)

type Store struct {
	db *bun.DB
}

func NewStore(db *bun.DB) *Store {
	return &Store{db: db}
}

func (s *Store) Initialize(ctx context.Context) error {
	return sqlutils.PostgresError(s.getMigrator().Up(ctx, s.db))
}

func (s *Store) Close() error {
	return s.db.Close()
}
