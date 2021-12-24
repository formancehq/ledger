package sqlstorage

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	"github.com/sirupsen/logrus"
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
	switch s.flavor {
	case sqlbuilder.PostgreSQL:
		return fmt.Sprintf(`"%s"."%s"`, s.ledger, name)
	default:
		return name
	}
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
	logrus.Debugln("initializing sqlite db")

	statements := make([]string, 0)

	migrationsDir := fmt.Sprintf("migrations/%s", strings.ToLower(s.flavor.String()))

	entries, err := migrations.ReadDir(migrationsDir)

	if err != nil {
		return err
	}

	for _, m := range entries {
		logrus.Debugf("running migrations %s\n", m.Name())

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
		logrus.Debugf("running statement: %s", statement)
		_, err = s.db.ExecContext(ctx, statement)

		if err != nil {
			err = fmt.Errorf("failed to run statement %d: %w", i, err)
			logrus.Errorln(err)
			return err
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
