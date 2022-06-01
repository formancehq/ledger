package sqlstorage

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/huandu/go-sqlbuilder"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
)

//go:embed migrations
var migrations embed.FS
var MigrationsFs fs.FS

func init() {
	// Just a trick to allow tests to override filesystem (dirty but it works)
	MigrationsFs = migrations
}

type Store struct {
	schema  Schema
	onClose func(ctx context.Context) error
}

func (s *Store) Schema() Schema {
	return s.schema
}

func (s *Store) error(err error) error {
	if err == nil {
		return nil
	}
	return errorFromFlavor(Flavor(s.schema.Flavor()), err)
}

func NewStore(schema Schema, onClose func(ctx context.Context) error) (*Store, error) {
	return &Store{
		schema:  schema,
		onClose: onClose,
	}, nil
}

func (s *Store) Name() string {
	return s.schema.Name()
}

func (s *Store) Initialize(ctx context.Context) (bool, error) {
	sharedlogging.GetLogger(ctx).Debug("Initialize store")

	migrationsDir := fmt.Sprintf("migrations/%s", strings.ToLower(s.schema.Flavor().String()))
	entries, err := fs.ReadDir(MigrationsFs, migrationsDir)
	if err != nil {
		return false, err
	}

	tx, err := s.schema.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return false, s.error(err)
	}
	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(tx)

	modified := false
	for _, m := range entries {
		version := strings.TrimSuffix(m.Name(), ".sql")

		sb := sqlbuilder.NewSelectBuilder()
		sb.Select("version")
		sb.From(s.schema.Table("migrations"))
		sb.Where(sb.E("version", version))

		// Does not use sql transaction because if the table does not exist, postgres will mark transaction as invalid
		sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
		row := s.schema.QueryRowContext(ctx, sqlq, args...)
		var v string
		if err = row.Scan(&v); err != nil {
			sharedlogging.GetLogger(ctx).Debugf("%s", err)
		}
		if v != "" {
			sharedlogging.GetLogger(ctx).Debugf("version %s already up to date", m.Name())
			continue
		}
		modified = true

		sharedlogging.GetLogger(ctx).Debugf("running migrations %s", m.Name())

		b, err := migrations.ReadFile(path.Join(migrationsDir, m.Name()))
		if err != nil {
			sharedlogging.GetLogger(ctx).Errorf("%s", err)
			return false, err
		}

		plain := strings.ReplaceAll(string(b), "VAR_LEDGER_NAME", s.schema.Name())
		r := regexp.MustCompile(`[\n\t\s]+`)
		plain = r.ReplaceAllString(plain, " ")

		for i, statement := range strings.Split(plain, "--statement ") {
			statement = strings.TrimSpace(statement)
			if statement != "" {
				sharedlogging.GetLogger(ctx).Debugf("running statement: %s", statement)
				if _, err = tx.ExecContext(ctx, statement); err != nil {
					err = errors.Wrapf(s.error(err), "failed to run statement %d", i)
					err = errors.Wrapf(s.error(err), "statement: %s", statement)
					sharedlogging.GetLogger(ctx).Errorf("%s", err)
					return false, s.error(err)
				}
			}
		}

		ib := sqlbuilder.NewInsertBuilder()
		ib.InsertInto(s.schema.Table("migrations"))
		ib.Cols("version", "date")
		ib.Values(version, time.Now().UTC().Format(time.RFC3339))
		sqlq, args = ib.BuildWithFlavor(s.schema.Flavor())
		if _, err = tx.ExecContext(ctx, sqlq, args...); err != nil {
			sharedlogging.GetLogger(ctx).Errorf("failed to insert migration version %s: %s", version, err)
			return false, s.error(err)
		}
	}

	return modified, s.error(tx.Commit())
}

func (s *Store) Close(ctx context.Context) error {
	return s.onClose(ctx)
}

var _ storage.Store = &Store{}
