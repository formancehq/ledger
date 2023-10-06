package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"time"

	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

const (
	// Keep goose name to keep backward compatibility
	migrationTable = "goose_db_version"
)

type Info struct {
	bun.BaseModel `bun:"goose_db_version"`

	Version string    `json:"version" bun:"version_id"`
	Name    string    `json:"name" bun:"-"`
	State   string    `json:"state,omitempty" bun:"-"`
	Date    time.Time `json:"date,omitempty" bun:"tstamp"`
}

type Migrator struct {
	migrations   []Migration
	schema       string
	createSchema bool
}

type option func(m *Migrator)

func WithSchema(schema string, create bool) option {
	return func(m *Migrator) {
		m.schema = schema
		m.createSchema = create
	}
}

func (m *Migrator) RegisterMigrations(migrations ...Migration) *Migrator {
	m.migrations = append(m.migrations, migrations...)
	return m
}

func (m *Migrator) createVersionTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`create table if not exists %s (
		id serial primary key,
		version_id bigint not null,
		is_applied boolean not null,
		tstamp timestamp default now()
	);`, migrationTable))
	if err != nil {
		return err
	}

	lastVersion, err := m.getLastVersion(ctx, tx)
	if err != nil {
		return err
	}

	if lastVersion == -1 {
		if err := m.insertVersion(ctx, tx, 0); err != nil {
			return err
		}
	}

	return err
}

func (m *Migrator) getLastVersion(ctx context.Context, querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}) (int64, error) {
	row := querier.QueryRowContext(ctx, fmt.Sprintf(`select max(version_id) from "%s";`, migrationTable))
	if err := row.Err(); err != nil {
		switch {
		case err == sql.ErrNoRows:
			return -1, nil
		default:
			switch err := err.(type) {
			case *pq.Error:
				switch err.Code {
				case "42P01": // Table not exists
					return -1, nil
				}
			}
		}

		return -1, errors.Wrap(err, "selecting max id from version table")
	}
	var number sql.NullInt64
	if err := row.Scan(&number); err != nil {
		return 0, err
	}

	if !number.Valid {
		return -1, nil
	}

	return number.Int64, nil
}

func (m *Migrator) insertVersion(ctx context.Context, tx bun.Tx, version int) error {
	_, err := tx.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO "%s" (version_id, is_applied, tstamp) VALUES (?, ?, ?)`, migrationTable),
		version, true, time.Now())
	return err
}

func (m *Migrator) GetDBVersion(ctx context.Context, db *bun.DB) (int64, error) {
	tx, err := m.newTx(ctx, db)
	if err != nil {
		return -1, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	return m.getLastVersion(ctx, tx)
}

func (m *Migrator) newTx(ctx context.Context, db bun.IDB) (bun.Tx, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return bun.Tx{}, err
	}

	if m.schema != "" {
		_, err := tx.ExecContext(ctx, fmt.Sprintf(`set search_path = "%s"`, m.schema))
		if err != nil {
			return bun.Tx{}, err
		}
	}

	return tx, err
}

func (m *Migrator) Up(ctx context.Context, db bun.IDB) error {
	tx, err := m.newTx(ctx, db)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if m.schema != "" && m.createSchema {
		_, err := tx.ExecContext(ctx, fmt.Sprintf(`create schema if not exists "%s"`, m.schema))
		if err != nil {
			return err
		}
	}

	if err := m.createVersionTable(ctx, tx); err != nil {
		return err
	}

	lastMigration, err := m.getLastVersion(ctx, tx)
	if err != nil {
		return err
	}

	if len(m.migrations) > int(lastMigration)-1 {
		for ind, migration := range m.migrations[lastMigration:] {
			if migration.UpWithContext != nil {
				if err := migration.UpWithContext(ctx, tx); err != nil {
					return err
				}
			} else if migration.Up != nil {
				if err := migration.Up(tx); err != nil {
					return err
				}
			} else {
				return errors.New("no code defined for migration")
			}

			if err := m.insertVersion(ctx, tx, int(lastMigration)+ind+1); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (m *Migrator) GetMigrations(ctx context.Context, db bun.IDB) ([]Info, error) {
	tx, err := m.newTx(ctx, db)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	migrationTableName := migrationTable
	if m.schema != "" {
		migrationTableName = fmt.Sprintf(`"%s".%s`, m.schema, migrationTableName)
	}

	ret := make([]Info, 0)
	if err := tx.NewSelect().
		TableExpr(migrationTableName).
		Order("version_id").
		Where("version_id >= 1").
		Column("version_id", "tstamp").
		Scan(ctx, &ret); err != nil {
		return nil, err
	}

	for i := 0; i < len(ret); i++ {
		ret[i].Name = m.migrations[i].Name
		ret[i].State = "DONE"
	}

	for i := len(ret); i < len(m.migrations); i++ {
		ret = append(ret, Info{
			Version: fmt.Sprint(i),
			Name:    m.migrations[i].Name,
			State:   "TO DO",
		})
	}

	return ret, nil
}

func (m *Migrator) IsUpToDate(ctx context.Context, db *bun.DB) (bool, error) {
	tx, err := m.newTx(ctx, db)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	version, err := m.getLastVersion(ctx, tx)
	if err != nil {
		return false, err
	}

	return int(version) == len(m.migrations), nil
}

func NewMigrator(opts ...option) *Migrator {
	ret := &Migrator{}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}
