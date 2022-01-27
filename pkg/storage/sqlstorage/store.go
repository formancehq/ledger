package sqlstorage

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/logging"
	"path"
	"strings"

	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/mattn/go-sqlite3"
)

//go:embed migrations
var migrations embed.FS

type Store struct {
	flavor  sqlbuilder.Flavor
	ledger  string
	db      *sql.DB
	onClose func(ctx context.Context) error
}

func (s *Store) table(name string) string {
	switch Flavor(s.flavor) {
	case PostgreSQL:
		return fmt.Sprintf(`"%s"."%s"`, s.ledger, name)
	default:
		return name
	}
}

func (s *Store) error(err error) error {
	return errorFromFlavor(Flavor(s.flavor), err)
}

func NewStore(name string, flavor sqlbuilder.Flavor, db *sql.DB, onClose func(ctx context.Context) error) (*Store, error) {
	return &Store{
		ledger:  name,
		db:      db,
		flavor:  flavor,
		onClose: onClose,
	}, nil
}

func (s *Store) Name() string {
	return s.ledger
}

func (s *Store) Initialize(ctx context.Context) error {
	logging.Debug(ctx, "initializing sqlite db")

	statements := make([]string, 0)

	migrationsDir := fmt.Sprintf("migrations/%s", strings.ToLower(s.flavor.String()))

	entries, err := migrations.ReadDir(migrationsDir)

	if err != nil {
		return s.error(err)
	}

	for _, m := range entries {
		logging.Debug(ctx, "running migrations %s", m.Name())

		b, err := migrations.ReadFile(path.Join(migrationsDir, m.Name()))
		if err != nil {
			return err
		}

		plain := strings.ReplaceAll(string(b), "VAR_LEDGER_NAME", s.ledger)

		statements = append(
			statements,
			strings.Split(plain, "--statement")...,
		)
	}

	for i, statement := range statements {
		logging.Debug(ctx, "running statement: %s", statement)
		_, err = s.db.ExecContext(ctx, statement)

		if err != nil {
			err = fmt.Errorf("failed to run statement %d: %w", i, err)
			logging.Error(ctx, "%s", err)
			return s.error(err)
		}
	}

	return nil
}

func (s *Store) Close(ctx context.Context) error {
	err := s.onClose(ctx)
	if err != nil {
		return err
	}
	return nil
}
