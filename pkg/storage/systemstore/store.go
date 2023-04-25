package systemstore

import (
	"context"

	storageerrors "github.com/formancehq/ledger/pkg/storage/errors"
	"github.com/formancehq/ledger/pkg/storage/schema"
)

type Store struct {
	schema schema.Schema
}

func NewStore(schema schema.Schema) *Store {
	return &Store{schema: schema}
}

func (s *Store) Initialize(ctx context.Context) error {
	if err := s.CreateLedgersTable(ctx); err != nil {
		return storageerrors.PostgresError(err)
	}

	return storageerrors.PostgresError(s.CreateConfigurationTable(ctx))
}
