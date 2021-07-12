package sqlite

import (
	"database/sql"
	"fmt"
	"log"
	"path"

	_ "github.com/mattn/go-sqlite3"
	"github.com/numary/ledger/core"
	"github.com/spf13/viper"
)

type SQLiteStore struct {
	ledger string
	db     *sql.DB
}

func NewStore(name string) (*SQLiteStore, error) {
	dbpath := fmt.Sprintf(
		"file:%s?_journal=WAL",
		path.Join(
			viper.GetString("storage.dir"),
			fmt.Sprintf("%s.db", viper.GetString("storage.sqlite.db_name")),
		),
	)

	db, err := sql.Open("sqlite3", dbpath)

	if err != nil {
		return nil, err
	}

	return &SQLiteStore{
		ledger: name,
		db:     db,
	}, nil
}

func (s *SQLiteStore) Initialize() error {
	log.Println("initializing sqlite db")

	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS transactions (
			"ledger"    varchar,
			"id"        integer,
			"timestamp" varchar,
			"reference" varchar,
			"hash"      varchar,

			UNIQUE("ledger", "id"),
			UNIQUE("ledger", "reference")
		);

		CREATE TABLE IF NOT EXISTS postings (
			"ledger"      varchar,
			"id"          integer,
			"txid"        integer,
			"source"      varchar,
			"destination" varchar,
			"amount"      integer,
			"asset"       varchar,

			UNIQUE("ledger", "id", "txid")
		);

		CREATE INDEX IF NOT EXISTS 'p_c0' ON "postings" (
			"ledger",
			"txid" DESC,
			"source",
			"destination"
		);

		CREATE TABLE IF NOT EXISTS metadata (
			"meta_id"          integer,
			"meta_target_type" varchar,
			"meta_target_id"   varchar,
			"meta_key"         varchar,
			"meta_value"       varchar,
			"timestamp"        varchar,
		
			UNIQUE("meta_id")
		);
		
		CREATE INDEX IF NOT EXISTS 'm_i0' ON "metadata" (
			"meta_target_type",
			"meta_target_id"
		);

		CREATE VIEW IF NOT EXISTS addresses AS SELECT ledger, address FROM (
			SELECT ledger, source as address FROM postings GROUP BY ledger, source
			UNION
			SELECT ledger, destination as address FROM postings GROUP BY ledger, destination
		) GROUP BY address, ledger;
	`)

	return err
}

func (s *SQLiteStore) Close() {
	s.db.Close()
	fmt.Println("db closed")
}

func (s *SQLiteStore) SaveTransactions(ts []core.Transaction) error {
	tx, _ := s.db.Begin()

	for _, t := range ts {
		var ref *string

		if t.Reference != "" {
			ref = &t.Reference
		}

		_, err := tx.Exec(
			`INSERT INTO "transactions"
				("ledger", "id", "reference", "timestamp", "hash")
			VALUES
				($1, $2, $3, $4, $5)
			`,
			s.ledger,
			t.ID,
			ref,
			t.Timestamp,
			t.Hash,
		)

		if err != nil {
			tx.Rollback()

			return err
		}

		for i, p := range t.Postings {
			_, err := tx.Exec(
				`
			INSERT INTO "postings"
				("ledger", "id", "txid", "source", "destination", "amount", "asset")
			VALUES
				(:ledger, :id, :txid, :source, :destination, :amount, :asset)
			`,
				sql.Named("ledger", s.ledger),
				sql.Named("id", i),
				sql.Named("txid", t.ID),
				sql.Named("source", p.Source),
				sql.Named("destination", p.Destination),
				sql.Named("amount", p.Amount),
				sql.Named("asset", p.Asset),
			)

			if err != nil {
				tx.Rollback()

				return err
			}
		}
	}

	return tx.Commit()
}
