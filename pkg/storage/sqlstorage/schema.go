package sqlstorage

import (
	"context"
	"database/sql"
	"fmt"
	"path"
)

type Schema struct {
	*sql.DB
	prefix  string
	closeDb bool
}

func (s *Schema) Table(name string) string {
	if s.prefix == "" {
		return name
	}
	return fmt.Sprintf(`"%s".%s`, s.prefix, name)
}

func (s *Schema) Close(ctx context.Context) error {
	if s.closeDb {
		return s.DB.Close()
	}
	return nil
}

type DB interface {
	Schema(ctx context.Context, name string) (*Schema, error)
	Close(ctx context.Context) error
}

type postgresDB struct {
	db *sql.DB
}

func (p *postgresDB) Schema(ctx context.Context, name string) (*Schema, error) {
	return &Schema{
		DB:     p.db,
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

type sqliteDB struct {
	directory string
	dbName    string
}

func (p *sqliteDB) Schema(ctx context.Context, name string) (*Schema, error) {
	path := path.Join(
		p.directory,
		fmt.Sprintf("%s_%s.schema", p.dbName, name),
	)
	db, err := OpenSQLDB(SQLite, path)
	if err != nil {
		return nil, err
	}

	return &Schema{
		DB:      db,
		closeDb: true,
	}, nil
}

func (p *sqliteDB) Close(ctx context.Context) error {
	return nil
}

func NewSQLiteDB(directory, dbName string) *sqliteDB {
	return &sqliteDB{
		directory: directory,
		dbName:    dbName,
	}
}
