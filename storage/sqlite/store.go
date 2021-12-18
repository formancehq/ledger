package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"github.com/sirupsen/logrus"
	"path"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed migration
var migrations embed.FS

type SQLiteStore struct {
	ledger string
	db     *sql.DB
}

func NewStore(storageDir, dbName, name string) (*SQLiteStore, error) {
	dbpath := fmt.Sprintf(
		"file:%s?_journal=WAL",
		path.Join(
			storageDir,
			fmt.Sprintf("%s_%s.db", dbName, name),
		),
	)

	logrus.Debugf("opening %s\n", dbpath)

	db, err := sql.Open("sqlite3", dbpath)

	if err != nil {
		return nil, err
	}

	return &SQLiteStore{
		ledger: name,
		db:     db,
	}, nil
}

func (s *SQLiteStore) Name() string {
	return s.ledger
}

func (s *SQLiteStore) Initialize(ctx context.Context) error {
	logrus.Debugln("initializing sqlite db")

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
		_, err = s.db.ExecContext(ctx, statement)

		if err != nil {
			err = fmt.Errorf("failed to run statement %d: %w", i, err)
			logrus.Errorln(err)
			return err
		}
	}

	return nil
}

func (s *SQLiteStore) Close(ctx context.Context) error {
	logrus.Debugln("sqlite db closed")
	err := s.db.Close()
	if err != nil {
		return err
	}
	return nil
}
