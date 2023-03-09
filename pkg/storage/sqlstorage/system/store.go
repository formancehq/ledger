package system

import (
	"context"

	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/schema"
)

type Store struct {
	schema schema.Schema
}

func NewStore(schema schema.Schema) *Store {
	return &Store{schema: schema}
}

func (s *Store) Initialize(ctx context.Context) error {
	if err := s.CreateLedgersTable(ctx); err != nil {
		return err
	}

	return s.CreateConfigurationTable(ctx)
}

func (s *Store) Close(ctx context.Context) error {
	return s.schema.Close(ctx)
}

var _ storage.SystemStore = &Store{}
