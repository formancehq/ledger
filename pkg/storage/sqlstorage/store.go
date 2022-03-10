package sqlstorage

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
	"path"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/mattn/go-sqlite3"
	"io/fs"
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
	sharedlogging.GetLogger(ctx).Debug("initializing schema")

	migrationsDir := fmt.Sprintf("migrations/%s", strings.ToLower(s.schema.Flavor().String()))
	entries, err := fs.ReadDir(MigrationsFs, migrationsDir)

	tx, err := s.schema.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return false, s.error(err)
	}
	defer tx.Rollback()

	modified := false
	for _, m := range entries {

		version := strings.TrimSuffix(m.Name(), ".sql")

		sb := sqlbuilder.NewSelectBuilder()
		sb.Select("version")
		sb.From(s.schema.Table("migrations"))
		sb.Where(sb.E("version", version))

		sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
		sharedlogging.GetLogger(ctx).Debug(sqlq, args)

		// Does not use sql transaction because if the table does not exists, postgres will mark transaction as invalid
		rows, err := s.schema.QueryContext(ctx, sqlq, args...)
		if err == nil && rows.Next() {
			rows.Close()
			sharedlogging.GetLogger(ctx).Debugf("Version %s already up to date", m.Name())
			rows.Close()
			continue
		}
		if rows != nil {
			rows.Close()
		}
		modified = true

		sharedlogging.GetLogger(ctx).Debugf("running migrations %s", m.Name())

		b, err := migrations.ReadFile(path.Join(migrationsDir, m.Name()))
		if err != nil {
			return false, err
		}

		plain := strings.ReplaceAll(string(b), "VAR_LEDGER_NAME", s.schema.Name())

		for i, statement := range strings.Split(plain, "--statement") {
			sharedlogging.GetLogger(ctx).Debugf("running statement: %s", statement)
			_, err = tx.ExecContext(ctx, statement)
			if err != nil {
				err = errors.Wrapf(s.error(err), "failed to run statement %d", i)
				sharedlogging.GetLogger(ctx).Errorf("%s", err)
				return false, err
			}
		}

		ib := sqlbuilder.NewInsertBuilder()
		ib.InsertInto(s.schema.Table("migrations"))
		ib.Cols("version", "date")
		ib.Values(version, time.Now())

		sqlq, args = ib.BuildWithFlavor(s.schema.Flavor())
		_, err = tx.ExecContext(ctx, sqlq, args...)
		if err != nil {
			return false, s.error(err)
		}
	}

	return modified, s.error(tx.Commit())
}

func (s *Store) Close(ctx context.Context) error {
	err := s.onClose(ctx)
	if err != nil {
		return err
	}
	return nil
}

var _ storage.Store = &Store{}
