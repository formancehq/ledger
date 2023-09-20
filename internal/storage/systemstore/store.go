package systemstore

import (
	"context"

	"github.com/formancehq/ledger/internal/storage"
	"github.com/uptrace/bun"
)

type Store struct {
	db *bun.DB
}

func NewStore(db *bun.DB) *Store {
	return &Store{db: db}
}

func (s *Store) Initialize(ctx context.Context) error {
	if err := s.CreateLedgersTable(ctx); err != nil {
		return storage.PostgresError(err)
	}

	return storage.PostgresError(s.CreateConfigurationTable(ctx))
}
