package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/numary/ledger/config"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger/query"
)

type PGStore struct {
	conn *pgx.Conn
}

func NewStore(c config.Config) (*PGStore, error) {
	var store *PGStore

	conn, err := pgx.Connect(
		context.Background(),
		"postgresql://localhost/postgres",
	)

	if err != nil {
		return store, err
	}

	store = &PGStore{
		conn: conn,
	}

	return store, nil
}

func (s *PGStore) Initialize() error {
	fmt.Println("init postgres")

	statements := `
		CREATE TABLE IF NOT EXISTS transactions (
			"id" integer,
			"timestamp" varchar,
			"reference" varchar,
			"hash" varchar,

			UNIQUE("id"),
			UNIQUE("reference")
		);

		CREATE TABLE IF NOT EXISTS postings (
			"id" integer,
			"txid" integer,
			"source" varchar,
			"destination" varchar,
			"amount" integer,
			"asset" varchar,

			UNIQUE("id", "txid")
		);

		CREATE INDEX IF NOT EXISTS p_c0 ON postings (
			"txid" DESC,
			"source",
			"destination"
		);

		CREATE TABLE IF NOT EXISTS metadata (
			"meta_id" integer,
			"meta_target_type" varchar,
			"meta_target_id" varchar,
			"meta_key" varchar,
			"meta_value" varchar,
			"timestamp" varchar,
		
			UNIQUE("meta_id")
		);
		
		CREATE INDEX IF NOT EXISTS m_i0 ON metadata (
			"meta_target_type",
			"meta_target_id"
		);
	`

	_, err := s.conn.Exec(context.Background(), statements)

	if err != nil {
		panic(err)
	}

	return err
}

func (s *PGStore) Close() {

}

func (s *PGStore) SaveTransactions(ts []core.Transaction) error {
	tx, _ := s.conn.Begin(context.Background())

	for _, t := range ts {
		var ref *string

		if t.Reference != "" {
			ref = &t.Reference
		}

		_, err := tx.Exec(context.Background(), `
		INSERT INTO "transactions"
			("id", "reference", "timestamp", "hash")
		VALUES
			($1, $2, $3, $4)
	`, t.ID, ref, t.Timestamp, t.Hash)

		if err != nil {
			tx.Rollback(context.Background())

			return err
		}

		for i, p := range t.Postings {
			_, err := tx.Exec(context.Background(),
				`
			INSERT INTO "postings"
				("id", "txid", "source", "destination", "amount", "asset")
			VALUES
				($1, $2, $3, $4, $5, $6)
			`,
				sql.Named("id", i),
				sql.Named("txid", t.ID),
				sql.Named("source", p.Source),
				sql.Named("destination", p.Destination),
				sql.Named("amount", p.Amount),
				sql.Named("asset", p.Asset),
			)

			if err != nil {
				tx.Rollback(context.Background())

				return err
			}
		}
	}

	return tx.Commit(context.Background())
}

func (s *PGStore) CountTransactions() (int64, error) {
	var count int64

	err := s.conn.QueryRow(
		context.Background(),
		`SELECT count(*) FROM transactions`,
	).Scan(&count)

	return count, err
}

func (s *PGStore) FindTransactions(query.Query) (query.Cursor, error) {
	c := query.Cursor{}
	results := []core.Transaction{}

	c.Data = results

	return c, nil
}

func (s *PGStore) AggregateBalances(string) (map[string]int64, error) {
	return map[string]int64{}, nil
}

func (s *PGStore) CountAccounts() (int64, error) {
	var count int64

	err := s.conn.QueryRow(
		context.Background(),
		`WITH addresses AS (
			SELECT "source" as address FROM postings
			UNION
			SELECT "destination" as address FROM postings
		)
		SELECT count(distinct address)
		FROM addresses`,
	).Scan(&count)

	return count, err
}

func (s *PGStore) FindAccounts(query.Query) (query.Cursor, error) {
	return query.Cursor{}, nil
}
