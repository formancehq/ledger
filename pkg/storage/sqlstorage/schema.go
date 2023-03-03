package sqlstorage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/huandu/go-sqlbuilder"
)

type Schema interface {
	executor
	Initialize(ctx context.Context) error
	Table(name string) string
	Close(ctx context.Context) error
	BeginTx(ctx context.Context, s *sql.TxOptions) (*sql.Tx, error)
	Flavor() sqlbuilder.Flavor
	Name() string
	Delete(ctx context.Context) error
}

type baseSchema struct {
	*sql.DB
	closeDb bool
	name    string
}

func (s *baseSchema) Name() string {
	return s.name
}

func (s *baseSchema) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	logging.FromContext(ctx).Debugf("QueryContext: %s %s", query, args)
	return s.DB.QueryContext(ctx, query, args...)
}
func (s *baseSchema) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	logging.FromContext(ctx).Debugf("QueryRowContext: %s %s", query, args)
	return s.DB.QueryRowContext(ctx, query, args...)
}
func (s *baseSchema) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	logging.FromContext(ctx).Debugf("ExecContext: %s %s", query, args)
	return s.DB.ExecContext(ctx, query, args...)
}
func (s *baseSchema) Close(ctx context.Context) error {
	if s.closeDb {
		return s.DB.Close()
	}
	return nil
}

func (s *baseSchema) Table(name string) string {
	return name
}

func (s *baseSchema) Initialize(ctx context.Context) error {
	return nil
}

type PGSchema struct {
	baseSchema
	prefix string
}

func (s *PGSchema) Table(name string) string {
	return fmt.Sprintf(`"%s".%s`, s.prefix, name)
}

func (s *PGSchema) Initialize(ctx context.Context) error {
	_, err := s.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS \"%s\"", s.name))
	return err
}

func (s *PGSchema) Flavor() sqlbuilder.Flavor {
	return sqlbuilder.PostgreSQL
}

func (s *PGSchema) Delete(ctx context.Context) error {
	_, err := s.ExecContext(ctx, fmt.Sprintf("DROP SCHEMA \"%s\" CASCADE", s.name))
	return err
}

func (s *PGSchema) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := s.baseSchema.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, postgresError(err)
	}
	return rows, nil
}

func (s *PGSchema) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	ret, err := s.baseSchema.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, postgresError(err)
	}
	return ret, nil
}

type DB interface {
	Initialize(ctx context.Context) error
	Schema(ctx context.Context, name string) (Schema, error)
	Close(ctx context.Context) error
}

type postgresDB struct {
	db *sql.DB
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
	return &PGSchema{
		baseSchema: baseSchema{
			DB:   p.db,
			name: name,
		},
		prefix: name,
	}, nil
}

func (p *postgresDB) Close(ctx context.Context) error {
	return p.db.Close()
}

func NewPostgresDB(db *sql.DB) *postgresDB {
	return &postgresDB{
		db: db,
	}
}
