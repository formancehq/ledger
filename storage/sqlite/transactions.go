package sqlite

import (
	"fmt"
	"math"
	"sort"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger/query"
	"github.com/spf13/viper"
)

func (s *SQLiteStore) FindTransactions(q query.Query) (query.Cursor, error) {
	q.Limit = int(math.Max(-1, math.Min(float64(q.Limit), 100))) + 1

	c := query.Cursor{}
	results := []core.Transaction{}

	in := sqlbuilder.NewSelectBuilder()
	in.Select("txid").From("postings")
	in.GroupBy("txid")
	in.OrderBy("txid desc")
	in.Limit(q.Limit)

	if q.After != "" {
		in.Where(in.LessThan("txid", q.After))
	}

	if q.HasParam("account") {
		in.Where(in.Or(
			in.Equal("source", q.Params["account"]),
			in.Equal("destination", q.Params["account"]),
		))
	}

	if q.HasParam("reference") {
		in.Where(
			in.Equal("reference", q.Params["reference"]),
		)
	}

	sb := sqlbuilder.NewSelectBuilder()
	sb.Select(
		"t.id",
		"t.timestamp",
		"t.hash",
		"t.reference",
		"p.source",
		"p.destination",
		"p.amount",
		"p.asset",
	)
	sb.From(sb.As("transactions", "t"))
	sb.Where(sb.In("t.id", in))
	sb.JoinWithOption(sqlbuilder.LeftJoin, sb.As("postings", "p"), "p.txid = t.id")
	sb.OrderBy("t.id desc, p.id asc")

	sqlq, args := sb.BuildWithFlavor(sqlbuilder.SQLite)
	if viper.GetBool("debug") {
		fmt.Println(sqlq, args)
	}

	rows, err := s.db.Query(
		sqlq,
		args...,
	)

	if err != nil {
		return c, err
	}

	transactions := map[int64]core.Transaction{}

	for rows.Next() {
		var txid int64
		var ts string
		var thash string
		// ref field can be NULL, treat it as an interface instead of string:
		var ref interface{}

		posting := core.Posting{}

		err := rows.Scan(
			&txid,
			&ts,
			&thash,
			&ref,
			&posting.Source,
			&posting.Destination,
			&posting.Amount,
			&posting.Asset,
		)
		if err != nil {
			return c, err
		}

		// Convert ref to string if it's available:
		var refStr string
		if ref != nil {
			refStr = ref.(string)
		}

		if _, ok := transactions[txid]; !ok {
			transactions[txid] = core.Transaction{
				ID:        txid,
				Postings:  []core.Posting{},
				Timestamp: ts,
				Hash:      thash,
				Reference: refStr,
				Metadata:  core.Metadata{},
			}
		}

		t := transactions[txid]
		t.AppendPosting(posting)
		transactions[txid] = t
	}

	for _, t := range transactions {
		meta, err := s.GetMeta("transaction", fmt.Sprintf("%d", t.ID))
		if err != nil {
			return c, err
		}
		t.Metadata = meta

		results = append(results, t)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].ID > results[j].ID
	})

	c.PageSize = q.Limit - 1

	c.HasMore = len(results) == q.Limit
	if c.HasMore {
		results = results[:len(results)-1]
	}
	c.Data = results

	total, _ := s.CountTransactions()
	c.Total = total

	return c, nil
}

func (s *SQLiteStore) SaveTransactions(ts []core.Transaction) error {
	tx, _ := s.db.Begin()

	for _, t := range ts {
		var ref *string

		if t.Reference != "" {
			ref = &t.Reference
		}

		ib := sqlbuilder.NewInsertBuilder()
		ib.InsertInto("transactions")
		ib.Cols("id", "reference", "timestamp", "hash")
		ib.Values(t.ID, ref, t.Timestamp, t.Hash)

		sqlq, args := ib.BuildWithFlavor(sqlbuilder.SQLite)

		_, err := tx.Exec(sqlq, args...)

		if err != nil {
			tx.Rollback()

			return err
		}

		for i, p := range t.Postings {
			ib := sqlbuilder.NewInsertBuilder()
			ib.InsertInto("postings")
			ib.Cols("id", "txid", "source", "destination", "amount", "asset")
			ib.Values(i, t.ID, p.Source, p.Destination, p.Amount, p.Asset)

			sqlq, args := ib.BuildWithFlavor(sqlbuilder.SQLite)

			_, err := tx.Exec(sqlq, args...)

			if err != nil {
				tx.Rollback()

				return err
			}
		}

		nextID, err := s.CountMeta()
		if err != nil {
			tx.Rollback()

			return err
		}

		for key, value := range t.Metadata {
			ib := sqlbuilder.NewInsertBuilder()
			ib.InsertInto("metadata")
			ib.Cols(
				"meta_id",
				"meta_target_type",
				"meta_target_id",
				"meta_key",
				"meta_value",
				"timestamp",
			)
			ib.Values(
				int(nextID),
				"transaction",
				fmt.Sprintf("%d", t.ID),
				key,
				string(value),
				t.Timestamp,
			)

			sqlq, args := ib.BuildWithFlavor(sqlbuilder.SQLite)
			if viper.GetBool("debug") {
				fmt.Println(sqlq, args)
			}

			_, err = tx.Exec(sqlq, args...)

			if err != nil {
				tx.Rollback()

				return err
			}

			nextID++
		}
	}

	return tx.Commit()
}

func (s *SQLiteStore) GetTransaction(txid string) (tx core.Transaction, err error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select(
		"t.id",
		"t.timestamp",
		"t.hash",
		"t.reference",
		"p.source",
		"p.destination",
		"p.amount",
		"p.asset",
	)
	sb.From(sb.As("transactions", "t"))
	sb.Where(sb.Equal("t.id", txid))
	sb.JoinWithOption(sqlbuilder.LeftJoin, sb.As("postings", "p"), "p.txid = t.id")
	sb.OrderBy("p.id asc")

	sqlq, args := sb.BuildWithFlavor(sqlbuilder.SQLite)
	if viper.GetBool("debug") {
		fmt.Println(sqlq, args)
	}

	rows, err := s.db.Query(
		sqlq,
		args...,
	)

	if err != nil {
		return tx, err
	}

	for rows.Next() {
		var txid int64
		var ts string
		var thash string
		var tref interface{}

		posting := core.Posting{}

		err := rows.Scan(
			&txid,
			&ts,
			&thash,
			&tref,
			&posting.Source,
			&posting.Destination,
			&posting.Amount,
			&posting.Asset,
		)
		if err != nil {
			return tx, err
		}

		tx.ID = txid
		tx.Timestamp = ts
		tx.Hash = thash
		tx.Metadata = core.Metadata{}
		if tref != nil {
			tx.Reference = tref.(string)
		}

		tx.AppendPosting(posting)
	}

	meta, err := s.GetMeta("transaction", fmt.Sprintf("%d", tx.ID))
	if err != nil {
		return tx, err
	}
	tx.Metadata = meta

	return tx, nil
}
