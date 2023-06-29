package storage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/uptrace/bun"
)

const (
	createSchemaQuery = `CREATE SCHEMA IF NOT EXISTS "%s"`
	deleteSchemaQuery = `DROP SCHEMA "%s" CASCADE`
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

func (s *Schema) Create(ctx context.Context) error {
	_, err := s.ExecContext(ctx, fmt.Sprintf(createSchemaQuery, s.name))
	return PostgresError(err)
}

func (s *Schema) Delete(ctx context.Context) error {
	_, err := s.ExecContext(ctx, fmt.Sprintf(deleteSchemaQuery, s.name))
	return PostgresError(err)
}

func (s *Schema) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	bunTx, err := s.IDB.BeginTx(ctx, opts)
	if err != nil {
		return nil, PostgresError(err)
	}
	return &Tx{
		schema: s,
		Tx:     bunTx,
	}, nil
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
