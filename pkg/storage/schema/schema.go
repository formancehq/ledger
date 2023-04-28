package schema

import (
	"context"
	"database/sql"
	"fmt"

	storageerrors "github.com/formancehq/ledger/pkg/storage/errors"
	"github.com/uptrace/bun"
)

type Schema struct {
	bun.IDB
	name string
}

func NewSchema(db bun.IDB, name string) Schema {
	return Schema{
		IDB:  db,
		name: name,
	}
}

func (s *Schema) Name() string {
	return s.name
}

func (s *Schema) Table(name string) string {
	return fmt.Sprintf(`"%s".%s`, s.name, name)
}

const (
	createSchemaQuery = `CREATE SCHEMA IF NOT EXISTS "%s"`
)

func (s *Schema) Create(ctx context.Context) error {
	_, err := s.ExecContext(ctx, fmt.Sprintf(createSchemaQuery, s.name))
	return storageerrors.PostgresError(err)
}

const (
	deleteSchemaQuery = `DROP SCHEMA "%s" CASCADE`
)

func (s *Schema) Delete(ctx context.Context) error {
	_, err := s.ExecContext(ctx, fmt.Sprintf(deleteSchemaQuery, s.name))
	return storageerrors.PostgresError(err)
}

func (s *Schema) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	bunTx, err := s.IDB.BeginTx(ctx, opts)
	if err != nil {
		return nil, storageerrors.PostgresError(err)
	}
	return &Tx{
		schema: s,
		Tx:     bunTx,
	}, nil
}

func (s *Schema) Flavor() string {
	return "postgres"
}

// Override all bun methods to use the schema name

func (s *Schema) NewInsert(tableName string) *bun.InsertQuery {
	return s.IDB.NewInsert().ModelTableExpr("?0.?1", bun.Ident(s.Name()), bun.Ident(tableName))
}

func (s *Schema) NewUpdate(tableName string) *bun.UpdateQuery {
	return s.IDB.NewUpdate().ModelTableExpr("?0.?1", bun.Ident(s.Name()), bun.Ident(tableName))
}

func (s *Schema) NewSelect(tableName string) *bun.SelectQuery {
	return s.IDB.NewSelect().ModelTableExpr("?0.?1 as ?1", bun.Ident(s.Name()), bun.Ident(tableName))
}

func (s *Schema) NewCreateTable(tableName string) *bun.CreateTableQuery {
	return s.IDB.NewCreateTable().ModelTableExpr("?0.?1", bun.Ident(s.Name()), bun.Ident(tableName))
}

func (s *Schema) NewDelete(tableName string) *bun.DeleteQuery {
	return s.IDB.NewDelete().ModelTableExpr("?0.?1", bun.Ident(s.Name()), bun.Ident(tableName))
}

type DB interface {
	Initialize(ctx context.Context) error
	Schema(name string) (Schema, error)
	Close(ctx context.Context) error
}

type postgresDB struct {
	db *bun.DB
}

func (p *postgresDB) Initialize(ctx context.Context) error {
	_, err := p.db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS pgcrypto")
	if err != nil {
		return storageerrors.PostgresError(err)
	}
	_, err = p.db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS pg_trgm")
	if err != nil {
		return storageerrors.PostgresError(err)
	}
	return nil
}

func (p *postgresDB) Schema(name string) (Schema, error) {
	return Schema{
		IDB:  p.db,
		name: name,
	}, nil
}

func (p *postgresDB) Close(ctx context.Context) error {
	return p.db.Close()
}

func NewPostgresDB(db *bun.DB) *postgresDB {
	return &postgresDB{
		db: db,
	}
}

type Tx struct {
	schema *Schema
	bun.Tx
}

func (s *Tx) NewSelect(tableName string) *bun.SelectQuery {
	return s.Tx.NewSelect().ModelTableExpr("?0.?1 as ?1", bun.Ident(s.schema.Name()), bun.Ident(tableName))
}

func (s *Tx) NewInsert(tableName string) *bun.InsertQuery {
	return s.Tx.NewInsert().ModelTableExpr("?0.?1 as ?1", bun.Ident(s.schema.Name()), bun.Ident(tableName))
}
