package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/formancehq/stack/libs/go-libs/time"

	"github.com/lib/pq"

	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

const (
	// Keep goose name to keep backward compatibility
	migrationTable = "goose_db_version"
)

var (
	ErrMissingVersionTable = errors.New("missing version table")
)

type Info struct {
	Version string    `json:"version" bun:"version_id"`
	Name    string    `json:"name" bun:"-"`
	State   string    `json:"state,omitempty" bun:"-"`
	Date    time.Time `json:"date,omitempty" bun:"tstamp"`
}

type Migrator struct {
	migrations   []Migration
	schema       string
	createSchema bool
	tableName    string
}

type option func(m *Migrator)

func WithSchema(schema string, create bool) option {
	return func(m *Migrator) {
		m.schema = schema
		m.createSchema = create
	}
}

func WithTableName(name string) option {
	return func(m *Migrator) {
		m.tableName = name
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
	);`, m.tableName))
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
	row := querier.QueryRowContext(ctx, fmt.Sprintf(`select max(version_id) from "%s";`, m.tableName))
	if err := row.Err(); err != nil {
		switch {
		case err == sql.ErrNoRows:
			return -1, nil
		default:
			switch err := err.(type) {
			case *pq.Error:
				switch err.Code {
				case "42P01": // Table not exists
					return -1, ErrMissingVersionTable
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
		fmt.Sprintf(`INSERT INTO "%s" (version_id, is_applied, tstamp) VALUES (?, ?, ?)`, m.tableName),
		version, true, time.Now())
	return err
}

func (m *Migrator) GetDBVersion(ctx context.Context, db bun.IDB) (int64, error) {
	ret := int64(0)
	if err := m.runInTX(ctx, db, func(ctx context.Context, tx bun.Tx) error {
		var err error
		ret, err = m.getLastVersion(ctx, tx)
		return err
	}); err != nil {
		return 0, err
	}

	return ret, nil
}

func (m *Migrator) runInTX(ctx context.Context, db bun.IDB, fn func(ctx context.Context, tx bun.Tx) error) error {
	return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		if m.schema != "" {
			_, err := tx.ExecContext(ctx, fmt.Sprintf(`set search_path = "%s"`, m.schema))
			if err != nil {
				return err
			}
		}
		return fn(ctx, tx)
	})
}

func (m *Migrator) Up(ctx context.Context, db bun.IDB) error {
	return m.runInTX(ctx, db, func(ctx context.Context, tx bun.Tx) error {
		if m.schema != "" && m.createSchema {
			_, err := tx.ExecContext(ctx, fmt.Sprintf(`create schema if not exists "%s"`, m.schema))
			if err != nil {
				return errors.Wrap(err, "creating schema")
			}
		}

		if err := m.createVersionTable(ctx, tx); err != nil {
			return errors.Wrap(err, "creating version table")
		}

		lastMigration, err := m.getLastVersion(ctx, tx)
		if err != nil {
			return errors.Wrap(err, "getting last migration")
		}

		if len(m.migrations) > int(lastMigration)-1 {
			for ind, migration := range m.migrations[lastMigration:] {
				if migration.UpWithContext != nil {
					if err := migration.UpWithContext(ctx, tx); err != nil {
						return err
					}
				} else if migration.Up != nil {
					if err := migration.Up(tx); err != nil {
						return errors.Wrapf(err, "executing migration %d", ind)
					}
				} else {
					return errors.New("no code defined for migration")
				}

				if err := m.insertVersion(ctx, tx, int(lastMigration)+ind+1); err != nil {
					return errors.Wrap(err, "inserting new version")
				}
			}
		}
		return nil
	})
}

func (m *Migrator) GetMigrations(ctx context.Context, db bun.IDB) ([]Info, error) {
	ret := make([]Info, 0)
	if err := m.runInTX(ctx, db, func(ctx context.Context, tx bun.Tx) error {
		migrationTableName := m.tableName
		if m.schema != "" {
			migrationTableName = fmt.Sprintf(`"%s".%s`, m.schema, migrationTableName)
		}

		if err := tx.NewSelect().
			TableExpr(migrationTableName).
			Order("version_id").
			Where("version_id >= 1").
			Column("version_id", "tstamp").
			Scan(ctx, &ret); err != nil {
			return err
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

		return nil
	}); err != nil {
		return nil, err
	}

	return ret, nil
}

func (m *Migrator) IsUpToDate(ctx context.Context, db *bun.DB) (bool, error) {
	ret := false
	if err := m.runInTX(ctx, db, func(ctx context.Context, tx bun.Tx) error {
		version, err := m.getLastVersion(ctx, tx)
		if err != nil {
			return err
		}

		ret = int(version) == len(m.migrations)
		return nil
	}); err != nil {
		return false, err
	}

	return ret, nil
}

func NewMigrator(opts ...option) *Migrator {
	ret := &Migrator{
		tableName: migrationTable,
	}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}
