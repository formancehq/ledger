package sqlite

import (
	"database/sql"
	"fmt"
	"strings"

	"numary.io/ledger/ledger/query"
)

func (s *SQLiteStore) queryTransactions(q query.Query) (string, []interface{}) {
	args := []interface{}{}
	conditions := []string{}

	if q.After != "" {
		conditions = append(conditions, "t.id < :after")
		args = append(args, sql.Named("after", q.After))
	}

	var where string
	if len(conditions) > 0 {
		where += "WHERE "
		where += strings.Join(conditions, " AND ")
	}

	sqlq := fmt.Sprintf(
		`WITH t AS (
			SELECT *
			FROM transactions t
			%s
			ORDER BY t.id DESC
			LIMIT :limit
		)
		SELECT t.id, t.timestamp, t.hash, p.source, p.destination, p.amount, p.asset
		FROM t
		LEFT JOIN "postings" p ON p.txid = t.id
		ORDER BY t.id DESC, p.id ASC`,
		where,
	)

	return sqlq, args
}

func (s *SQLiteStore) queryAccountTransactions(q query.Query) (string, []interface{}) {
	args := []interface{}{}

	var where string
	if q.After != "" {
		where = "AND txid < :after"
		args = append(args, sql.Named("after", q.After))
	}

	sqlq := fmt.Sprintf(
		`WITH _p AS (
			SELECT txid
			FROM postings
			WHERE (
				source = :account
				OR destination = :account
			)
			%s
			GROUP BY txid
			LIMIT :limit
		), t AS (
			SELECT *
			FROM transactions t
			INNER JOIN _p ON _p.txid = t.id
			ORDER BY t.id DESC
		)
		SELECT
			t.id,
			t.timestamp,
			t.hash,
			p.source,
			p.destination,
			p.amount,
			p.asset
		FROM t
		LEFT JOIN "postings" p ON p.txid = t.id
		ORDER BY t.id DESC, p.id ASC`,
		where,
	)

	args = append(args, sql.Named("account", q.Params["account"]))

	return sqlq, args
}
