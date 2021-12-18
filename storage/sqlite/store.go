package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"log"
	"path"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/viper"
)

//go:embed migration
var migrations embed.FS

type SQLiteStore struct {
	ledger string
	db     *sql.DB
}

func NewStore(name string) (*SQLiteStore, error) {
	dbpath := fmt.Sprintf(
		"file:%s?_journal=WAL",
		path.Join(
			viper.GetString("storage.dir"),
			fmt.Sprintf(
				"%s_%s.db",
				viper.GetString("storage.sqlite.db_name"),
				name,
			),
		),
	)

	log.Printf("opening %s\n", dbpath)

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
	log.Println("initializing sqlite db")

	statements := []string{}

	entries, err := migrations.ReadDir("migration")

	if err != nil {
		return err
	}

	for _, m := range entries {
		log.Printf("running migration %s\n", m.Name())

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
			fmt.Println(err)
			err = fmt.Errorf("failed to run statement %d: %w", i, err)
			return err
		}
	}

	return nil
}

func (s *SQLiteStore) Close(ctx context.Context) error {
	log.Println("sqlite db closed")
	err := s.db.Close()
	if err != nil {
		return err
	}
	return nil
}
