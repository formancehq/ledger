package postgres

import (
	"context"
	"embed"
	"fmt"
	"log"
	"path"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/spf13/viper"
)

//go:embed migration
var migrations embed.FS

type PGStore struct {
	ledger     string
	connString string
	conn       *pgx.Conn
}

func (s *PGStore) connect() error {
	log.Println("initiating postgres connection")

	conn, err := pgx.Connect(
		context.TODO(),
		s.connString,
	)

	if err != nil {
		return err
	}

	s.conn = conn

	return nil
}

func (s *PGStore) Conn() *pgx.Conn {
	if s.conn.IsClosed() {
		err := s.connect()

		if err != nil {
			panic(err)
		}
	}

	return s.conn
}

func NewStore(name string) (*PGStore, error) {
	store := &PGStore{
		ledger:     name,
		connString: viper.GetString("storage.postgres.conn_string"),
	}

	err := store.connect()

	if err != nil {
		return store, err
	}

	return store, nil
}

func (s *PGStore) Initialize() error {
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
		_, err = s.Conn().Exec(
			context.Background(),
			statement,
		)

		if err != nil {
			fmt.Println(err)
			err = fmt.Errorf("failed to run statement %d: %w", i, err)
			return err
		}
	}

	return nil
}

func (s *PGStore) table(name string) string {
	return fmt.Sprintf("%s.%s", s.ledger, name)
}

func (s *PGStore) Close() {
	s.conn.Close(context.TODO())
}
