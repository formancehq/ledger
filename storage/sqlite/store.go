package sqlite

import (
	"database/sql"
	"fmt"
	"math"
	"path"
	"sort"

	_ "github.com/mattn/go-sqlite3"
	"numary.io/ledger/config"
	"numary.io/ledger/core"
	"numary.io/ledger/ledger/query"
)

type SQLiteStore struct {
	db       *sql.DB
	prepared map[string]*sql.Stmt
}

func NewStore(c config.Config) (*SQLiteStore, error) {
	dbpath := fmt.Sprintf(
		"file:%s?_journal=WAL",
		path.Join(
			c.Storage.SQLiteOpts.Directory,
			fmt.Sprintf("%s.db", c.Storage.SQLiteOpts.DBName),
		),
	)

	db, err := sql.Open("sqlite3", dbpath)

	if err != nil {
		return nil, err
	}

	return &SQLiteStore{
		db: db,
	}, nil
}

func (s *SQLiteStore) Initialize() error {
	_, err := s.db.Exec(`
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

		CREATE INDEX IF NOT EXISTS 'p_c0' ON "postings" (
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
		
		CREATE INDEX IF NOT EXISTS 'm_i0' ON "metadata" (
			"meta_target_type",
			"meta_target_id"
		);
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

		_, err := tx.Exec(`
		INSERT INTO "transactions"
			("id", "reference", "timestamp", "hash")
		VALUES
			($1, $2, $3, $4)
	`, t.ID, ref, t.Timestamp, t.Hash)

		if err != nil {
			tx.Rollback()

			return err
		}

		for i, p := range t.Postings {
			_, err := tx.Exec(
				`
			INSERT INTO "postings"
				("id", "txid", "source", "destination", "amount", "asset")
			VALUES
				(:id, :txid, :source, :destination, :amount, :asset)
			`,
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

func (s *SQLiteStore) CountTransactions() (int64, error) {
	var count int64

	err := s.db.QueryRow(`SELECT count(*) FROM transactions`).Scan(&count)

	return count, err
}

func (s *SQLiteStore) FindTransactions(q query.Query) (query.Cursor, error) {
	c := query.Cursor{}

	results := []core.Transaction{}

	var sqlq string
	var args []interface{}

	if q.HasParam("account") {
		sqlq, args = s.queryAccountTransactions(q)
	} else {
		sqlq, args = s.queryTransactions(q)
	}

	limit := int(math.Max(-1, math.Min(float64(q.Limit), 100)))
	args = append(args, sql.Named("limit", limit))

	rows, err := s.db.Query(
		sqlq,
		args...,
	)

	if err != nil {
		fmt.Println(err)
		return c, err
	}

	transactions := map[int64]core.Transaction{}

	for rows.Next() {
		var txid int64
		var ts string
		var thash string

		posting := core.Posting{}

		err = rows.Scan(
			&txid,
			&ts,
			&thash,
			&posting.Source,
			&posting.Destination,
			&posting.Amount,
			&posting.Asset,
		)

		if err != nil {
			return c, err
		}

		if err != nil {
			return c, err
		}

		if _, ok := transactions[txid]; !ok {
			transactions[txid] = core.Transaction{
				ID:        txid,
				Postings:  []core.Posting{},
				Timestamp: ts,
				Hash:      thash,
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

	c.PageSize = q.Limit
	c.HasMore = len(results) >= 1 && results[len(results)-1].ID > 0
	c.Data = results

	return c, nil
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

func (s *SQLiteStore) FindAccounts(q query.Query) (query.Cursor, error) {
	c := query.Cursor{}

	results := []core.Account{}

	var where string
	if q.After != "" {
		where = "WHERE address < :after"
	}

	var total int

	err := s.db.QueryRow(
		fmt.Sprintf(`
			WITH addresses AS (
				SELECT "source" as address FROM postings
				UNION
				SELECT "destination" as address FROM postings
			)
			SELECT count(DISTINCT address)
			FROM addresses
			%s
		`, where),
		sql.Named("after", q.After),
	).Scan(&total)

	rows, err := s.db.Query(
		fmt.Sprintf(`
			WITH addresses AS (
				SELECT "source" as address FROM postings
				UNION
				SELECT "destination" as address FROM postings
			)
			SELECT address
			FROM addresses
			%s
			GROUP BY address
			ORDER BY address DESC
			LIMIT :limit
		`, where),
		sql.Named("limit", q.Limit),
		sql.Named("after", q.After),
	)

	if err != nil {
		return c, err
	}

	for rows.Next() {
		var address string

		err := rows.Scan(&address)

		if err != nil {
			return c, err
		}

		results = append(results, core.Account{
			Address:  address,
			Contract: "default",
		})
	}

	c.PageSize = q.Limit
	c.HasMore = len(results) < total
	c.Remaning = total
	c.Data = results

	return c, nil
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
