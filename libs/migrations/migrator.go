package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

const (
	// Keep goose name to keep backward compatibility
	migrationTable = "goose_db_version"
)

type Migrator struct {
	migrations []Migration
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
		if err == sql.ErrNoRows {
			return -1, nil
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
	return m.getLastVersion(ctx, db)
}

func (m *Migrator) Up(ctx context.Context, db *bun.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := m.createVersionTable(ctx, tx); err != nil {
		return err
	}

	lastMigration, err := m.getLastVersion(ctx, tx)
	if err != nil {
		return err
	}

	for ind, migration := range m.migrations[lastMigration:] {
		if err := migration.Up(tx); err != nil {
			return err
		}

		if err := m.insertVersion(ctx, tx, ind+1); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func NewMigrator() *Migrator {
	return &Migrator{}
}
