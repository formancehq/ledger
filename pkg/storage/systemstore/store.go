package systemstore

import (
	"context"

	"github.com/formancehq/ledger/pkg/storage"
)

type Store struct {
	schema storage.Schema
}

func NewStore(schema storage.Schema) *Store {
	return &Store{schema: schema}
}

func (s *Store) Initialize(ctx context.Context) error {
	if err := s.CreateLedgersTable(ctx); err != nil {
		return storage.PostgresError(err)
	}

	return storage.PostgresError(s.CreateConfigurationTable(ctx))
}
