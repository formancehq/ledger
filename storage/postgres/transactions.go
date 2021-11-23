package postgres

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger/query"
	"github.com/spf13/viper"
)

func (s *PGStore) SaveTransactions(ts []core.Transaction) error {
	tx, _ := s.Conn().Begin(context.Background())

	for _, t := range ts {
		var ref *string

		if t.Reference != "" {
			ref = &t.Reference
		}

		ib := sqlbuilder.NewInsertBuilder()
		ib.InsertInto(s.table("transactions"))
		ib.Cols("id", "reference", "timestamp", "hash")
		ib.Values(t.ID, ref, t.Timestamp, t.Hash)

		sqlq, args := ib.BuildWithFlavor(sqlbuilder.PostgreSQL)

		_, err := tx.Exec(
			context.Background(),
			sqlq,
			args...,
		)

		if err != nil {
			tx.Rollback(context.Background())

			return err
		}

		for i, p := range t.Postings {
			ib := sqlbuilder.NewInsertBuilder()
			ib.InsertInto(s.table("postings"))
			ib.Cols("id", "txid", "source", "destination", "amount", "asset")
			ib.Values(i, t.ID, p.Source, p.Destination, p.Amount, p.Asset)

			sqlq, args := ib.BuildWithFlavor(sqlbuilder.PostgreSQL)

			_, err := tx.Exec(
				context.Background(),
				// `INSERT INTO "postings"
				// 	("id", "txid", "source", "destination", "amount", "asset")
				// VALUES
				// 	($1, $2, $3, $4, $5, $6)`,
				// i,
				// t.ID,
				// p.Source,
				// p.Destination,
				// p.Amount,
				// p.Asset,
				sqlq,
				args...,
			)

			if err != nil {
				tx.Rollback(context.Background())

				return err
			}
		}

		nextID, err := s.CountMeta()
		if err != nil {
			tx.Rollback(context.Background())

			return err
		}

		for key, value := range t.Metadata {
			ib := sqlbuilder.NewInsertBuilder()
			ib.InsertInto(s.table("metadata"))
			ib.Cols(
				"meta_id",
				"meta_target_type",
				"meta_target_id",
				"meta_key",
				"meta_value",
			)
			ib.Values(
				int(nextID),
				"transaction",
				fmt.Sprintf("%d", t.ID),
				key,
				string(value),
			)

			sqlq, args := ib.BuildWithFlavor(sqlbuilder.PostgreSQL)

			_, err = tx.Exec(context.Background(), sqlq, args...)

			if err != nil {
				tx.Rollback(context.Background())

				return err
			}

			nextID++
		}
	}

	return tx.Commit(context.Background())
}

func (s *PGStore) CountTransactions() (int64, error) {
	var count int64

	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("count(*)")
	sb.From(s.table("transactions"))

	sqlq, args := sb.BuildWithFlavor(sqlbuilder.PostgreSQL)

	err := s.Conn().QueryRow(
		context.Background(),
		sqlq,
		args...,
	).Scan(&count)

	return count, err
}

func (s *PGStore) FindTransactions(q query.Query) (query.Cursor, error) {
	q.Limit = int(math.Max(-1, math.Min(float64(q.Limit), 100)))

	c := query.Cursor{}
	results := []core.Transaction{}

	in := sqlbuilder.NewSelectBuilder()
	in.Select("txid").From(s.table("postings"))
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
	sb.From(sb.As(s.table("transactions"), "t"))
	sb.Where(sb.In("t.id", in))
	sb.JoinWithOption(sqlbuilder.LeftJoin, sb.As(s.table("postings"), "p"), "p.txid = t.id")
	sb.OrderBy("t.id desc, p.id asc")

	sqlq, args := sb.BuildWithFlavor(sqlbuilder.PostgreSQL)

	rows, err := s.Conn().Query(
		context.Background(),
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
		var tref string

		posting := core.Posting{}

		rows.Scan(
			&txid,
			&ts,
			&thash,
			&tref,
			&posting.Source,
			&posting.Destination,
			&posting.Amount,
			&posting.Asset,
		)

		if _, ok := transactions[txid]; !ok {
			transactions[txid] = core.Transaction{
				ID:        txid,
				Postings:  []core.Posting{},
				Timestamp: ts,
				Hash:      thash,
				Reference: tref,
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

	c.Data = results

	return c, nil
}

func (s *PGStore) GetTransaction(id string) (core.Transaction, error) {
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
	sb.Where(sb.Equal("t.id", id))
	sb.JoinWithOption(sqlbuilder.LeftJoin, sb.As("postings", "p"), "p.txid = t.id")
	sb.OrderBy("p.id asc")

	sqlq, args := sb.BuildWithFlavor(sqlbuilder.PostgreSQL)
	if viper.GetBool("debug") {
		fmt.Println(sqlq, args)
	}

	tx := core.Transaction{}

	rows, err := s.Conn().Query(
		context.Background(),
		sqlq,
		args...,
	)

	if err != nil {
		return tx, err
	}

	txFieldsSet := false

	for rows.Next() {
		var txid int64
		var ts string
		var thash string
		var tref string

		posting := core.Posting{}

		rows.Scan(
			&txid,
			&ts,
			&thash,
			&tref,
			&posting.Source,
			&posting.Destination,
			&posting.Amount,
			&posting.Asset,
		)

		if !txFieldsSet {
			tx.ID = txid
			tx.Postings = []core.Posting{}
			tx.Timestamp = ts
			tx.Hash = thash
			tx.Reference = tref
			tx.Metadata = core.Metadata{}

			txFieldsSet = true
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
