package sqlite

import (
	"database/sql"
	"fmt"
	"math"
	"sort"

	_ "github.com/mattn/go-sqlite3"
	"numary.io/ledger/core"
	"numary.io/ledger/ledger/query"
)

type SQLiteStore struct {
	db *sql.DB
}

func NewStore() (*SQLiteStore, error) {
	db, err := sql.Open("sqlite3", "file:/tmp/ledger.db?_journal=WAL")

	if err != nil {
		return nil, err
	}

	return &SQLiteStore{
		db,
	}, nil
}

func (s *SQLiteStore) Initialize() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS transactions (
			"id" integer primary key,
			"timestamp" varchar,
			"hash" varchar,
			"metadata" varchar
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
	`)

	return err
}

func (s *SQLiteStore) Close() {
	s.db.Close()
	fmt.Println("db closed")
}

func (s *SQLiteStore) AppendTransaction(t core.Transaction) error {
	tx, _ := s.db.Begin()

	for i, p := range t.Postings {
		tx.Exec(`
			INSERT INTO "transactions"
				("id", "timestamp")
			VALUES
				($1, $2)
		`, t.ID, t.Timestamp)
		tx.Exec(`
			INSERT INTO "postings"
				("id", "txid", "source", "destination", "amount", "asset")
			VALUES
				($1, $2, $3, $4, $5, $6)
		`, i, t.ID, p.Source, p.Destination, p.Amount, p.Asset)
	}

	return tx.Commit()
}

func (s *SQLiteStore) CountTransactions() (int64, error) {
	var count int64

	err := s.db.QueryRow(`SELECT count(*) FROM transactions`).Scan(&count)

	return count, err
}

func (s *SQLiteStore) FindTransactions(q query.Query) ([]core.Transaction, error) {
	results := []core.Transaction{}

	limit := int(math.Max(-1, math.Min(float64(q.Limit), 100)))

	rows, err := s.db.Query(`
		WITH t AS (
			SELECT *
			FROM transactions t
			LIMIT $1
		)
		SELECT t.id, t.timestamp, p.source, p.destination, p.amount, p.asset
		FROM t
		LEFT JOIN "postings" p ON p.txid = t.id
		ORDER BY t.id DESC, p.id ASC
	`, limit)

	if err != nil {
		fmt.Println(err)
		return results, err
	}

	transactions := map[int64]core.Transaction{}

	for rows.Next() {
		var txid int64
		var ts string

		posting := core.Posting{}

		err = rows.Scan(
			&txid,
			&ts,
			&posting.Source,
			&posting.Destination,
			&posting.Amount,
			&posting.Asset,
		)

		if err != nil {
			return results, err
		}

		if err != nil {
			return results, err
		}

		if _, ok := transactions[txid]; !ok {
			transactions[txid] = core.Transaction{
				ID:        txid,
				Postings:  []core.Posting{},
				Timestamp: ts,
			}
		}

		t := transactions[txid]
		t.AppendPosting(posting)
		transactions[txid] = t
	}

	for _, t := range transactions {
		results = append(results, t)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].ID > results[j].ID
	})

	return results, nil
}

func (s *SQLiteStore) CountAccounts() (int64, error) {
	var count int64

	err := s.db.QueryRow(`
		WITH addresses AS (
			SELECT "source" as address FROM postings
			UNION
			SELECT "destination" as address FROM postings
		)
		SELECT count(distinct address)
		FROM addresses
	`).Scan(&count)

	return count, err
}

func (s *SQLiteStore) FindAccounts(q query.Query) ([]core.Account, error) {
	results := []core.Account{}

	rows, err := s.db.Query(`
		WITH addresses AS (
			SELECT "source" as address FROM postings
			UNION
			SELECT "destination" as address FROM postings
		)
		SELECT address
		FROM addresses
		GROUP BY address
		LIMIT $1
	`, q.Limit)

	if err != nil {
		return results, err
	}

	for rows.Next() {
		var address string

		err := rows.Scan(&address)

		if err != nil {
			return results, err
		}

		results = append(results, core.Account{
			Address:  address,
			Contract: "default",
		})
	}

	return results, nil
}

func (s *SQLiteStore) FindPostings(q query.Query) ([]core.Posting, error) {
	res := []core.Posting{}

	limit := q.Limit

	rows, err := s.db.Query(`
		SELECT
			p.source, p.destination, p.amount, p.asset
		FROM postings p
		LIMIT $1
	`, limit)

	if err != nil {
		return res, err
	}

	for rows.Next() {
		var posting core.Posting

		err := rows.Scan(
			&posting.Source,
			&posting.Destination,
			&posting.Amount,
			&posting.Asset,
		)

		if err != nil {
			return res, err
		}

		res = append(res, posting)
	}

	return res, nil
}

func (s *SQLiteStore) AggregateBalances(address string) (map[string]int64, error) {
	balances := map[string]int64{}

	rows, err := s.db.Query(`
		WITH assets AS (
			SELECT asset, 'out', SUM(amount)
			FROM postings
			WHERE source = $1
			GROUP BY asset
			UNION
			SELECT asset, 'in', SUM(amount)
			FROM postings
			WHERE destination = $1
			GROUP BY asset
		)
		SELECT *
		FROM assets
		;
	`, address)

	if err != nil {
		return balances, err
	}

	for rows.Next() {
		var row = struct {
			asset  string
			t      string
			amount int64
		}{}

		err := rows.Scan(&row.asset, &row.t, &row.amount)

		if err != nil {
			return balances, err
		}

		if row.t == "out" {
			balances[row.asset] -= row.amount
		} else {
			balances[row.asset] += row.amount
		}
	}

	return balances, nil
}
