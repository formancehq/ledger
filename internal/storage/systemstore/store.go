package systemstore

import (
	"context"
	"fmt"

	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/formancehq/ledger/v2/internal/storage/sqlutils"

	"github.com/uptrace/bun"
)

const Schema = "_system"

type Store struct {
	db *bun.DB
}

func Connect(ctx context.Context, connectionOptions bunconnect.ConnectionOptions, hooks ...bun.QueryHook) (*Store, error) {

	db, err := bunconnect.OpenDBWithSchema(ctx, connectionOptions, Schema, hooks...)
	if err != nil {
		return nil, sqlutils.PostgresError(err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(`create schema if not exists "%s"`, Schema))
	if err != nil {
		return nil, sqlutils.PostgresError(err)
	}

	return &Store{db: db}, nil
}

func (s *Store) DB() *bun.DB {
	return s.db
}

func (s *Store) Close() error {
	return s.db.Close()
}
