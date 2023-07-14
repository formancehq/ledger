package storage

import (
	"context"

	"github.com/uptrace/bun"
)

type Database struct {
	db *bun.DB
}

func (p *Database) Initialize(ctx context.Context) error {
	_, err := p.db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS pgcrypto")
	if err != nil {
		return PostgresError(err)
	}
	_, err = p.db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS pg_trgm")
	if err != nil {
		return PostgresError(err)
	}
	return nil
}

func (p *Database) Schema(name string) (Schema, error) {
	return Schema{
		IDB:  p.db,
		name: name,
	}, nil
}

func NewDatabase(db *bun.DB) *Database {
	return &Database{
		db: db,
	}
}
