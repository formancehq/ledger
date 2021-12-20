package postgres

import (
	"context"
	"embed"
	"fmt"
	"github.com/sirupsen/logrus"
	"path"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"
)

//go:embed migration
var migrations embed.FS

type PGStore struct {
	ledger string
	pool   *pgxpool.Pool
}

func (s *PGStore) Conn() *pgxpool.Pool {
	return s.pool
}

func NewStore(name string, pool *pgxpool.Pool) (*PGStore, error) {
	return &PGStore{
		ledger: name,
		pool:   pool,
	}, nil
}

func (s *PGStore) Name() string {
	return s.ledger
}

func (s *PGStore) Initialize(ctx context.Context) error {
	statements := []string{}

	entries, err := migrations.ReadDir("migration")

	if err != nil {
		return err
	}

	for _, m := range entries {
		logrus.Debugf("running migration %s\n", m.Name())

		b, err := migrations.ReadFile(path.Join("migration", m.Name()))

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
		_, err = s.Conn().Exec(
			ctx,
			statement,
		)

		if err != nil {
			err = fmt.Errorf("failed to run statement %d: %w", i, err)
			logrus.Errorf("error running statement: %s", err)
			return err
		}
	}

	return nil
}

func (s *PGStore) table(name string) string {
	return fmt.Sprintf(`"%s"."%s"`, s.ledger, name)
}

func (s *PGStore) Close(ctx context.Context) error {
	return nil
}

func (s *PGStore) DropTest() {
	s.Conn().Exec(
		context.Background(),
		"DROP SCHEMA test CASCADE",
	)
}
