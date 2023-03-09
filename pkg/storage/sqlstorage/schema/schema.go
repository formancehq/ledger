package schema

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
)

type Schema struct {
	*bun.DB
	name string
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

func (s *Schema) Initialize(ctx context.Context) error {
	_, err := s.ExecContext(ctx, fmt.Sprintf(createSchemaQuery, s.name))
	return err
}

const (
	deleteSchemaQuery = `DROP SCHEMA "%s" CASCADE`
)

func (s *Schema) Delete(ctx context.Context) error {
	_, err := s.ExecContext(ctx, fmt.Sprintf(deleteSchemaQuery, s.name))
	return err
}

func (s *Schema) Flavor() string {
	return "postgres"
}

func (s *Schema) Close(ctx context.Context) error {
	// Do not close the DB, it is shared with other schemas
	return nil
}

// Override all bun methods to use the schema name

func (s *Schema) NewInsert(tableName string) *bun.InsertQuery {
	return s.DB.NewInsert().ModelTableExpr("?0.?1", bun.Ident(s.Name()), bun.Ident(tableName))
}

func (s *Schema) NewUpdate(tableName string) *bun.UpdateQuery {
	return s.DB.NewUpdate().ModelTableExpr("?0.?1", bun.Ident(s.Name()), bun.Ident(tableName))
}

func (s *Schema) NewSelect(tableName string) *bun.SelectQuery {
	return s.DB.NewSelect().ModelTableExpr("?0.?1 as ?1", bun.Ident(s.Name()), bun.Ident(tableName))
}

func (s *Schema) NewCreateTable(tableName string) *bun.CreateTableQuery {
	return s.DB.NewCreateTable().ModelTableExpr("?0.?1", bun.Ident(s.Name()), bun.Ident(tableName))
}

func (s *Schema) NewDelete(tableName string) *bun.DeleteQuery {
	return s.DB.NewDelete().ModelTableExpr("?0.?1", bun.Ident(s.Name()), bun.Ident(tableName))
}

type DB interface {
	Initialize(ctx context.Context) error
	Schema(ctx context.Context, name string) (Schema, error)
	Close(ctx context.Context) error
}

type postgresDB struct {
	db *bun.DB
}

func (p *postgresDB) Initialize(ctx context.Context) error {
	_, err := p.db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS pgcrypto")
	if err != nil {
		return err
	}
	_, err = p.db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS pg_trgm")
	if err != nil {
		return err
	}
	return nil
}

func (p *postgresDB) Schema(ctx context.Context, name string) (Schema, error) {
	return Schema{
		DB:   p.db,
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
