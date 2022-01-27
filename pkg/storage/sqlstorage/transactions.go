package sqlstorage

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/numary/ledger/pkg/storage"
	"github.com/sirupsen/logrus"
	"math"
	"sort"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
)

func (s *Store) FindTransactions(ctx context.Context, q query.Query) (query.Cursor, error) {
	q.Limit = int(math.Max(-1, math.Min(float64(q.Limit), 100))) + 1

	c := query.Cursor{}
	results := make([]core.Transaction, 0)

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

	sqlq, args := sb.BuildWithFlavor(s.flavor)

	rows, err := s.db.QueryContext(
		ctx,
		sqlq,
		args...,
	)

	if err != nil {
		return c, s.error(err)
	}

	transactions := map[int64]core.Transaction{}

	for rows.Next() {
		var txid int64
		var ts string
		var thash string
		var ref sql.NullString

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

		if _, ok := transactions[txid]; !ok {
			transactions[txid] = core.Transaction{
				ID:        txid,
				Postings:  []core.Posting{},
				Timestamp: ts,
				Hash:      thash,
				Reference: ref.String,
				Metadata:  core.Metadata{},
			}
		}

		t := transactions[txid]
		t.AppendPosting(posting)
		transactions[txid] = t
	}

	for _, t := range transactions {
		meta, err := s.GetMeta(ctx, "transaction", fmt.Sprintf("%d", t.ID))
		if err != nil {
			return c, s.error(err)
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

	total, _ := s.CountTransactions(ctx)
	c.Total = total

	return c, nil
}

func (s *Store) SaveTransactions(ctx context.Context, ts []core.Transaction) (map[int]error, error) {

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, s.error(err)
	}

	mustRollback := false
	ret := make(map[int]error)

txLoop:
	for i, t := range ts {
		var ref *string

		if t.Reference != "" {
			ref = &t.Reference
		}

		commitError := func(err error) {
			mustRollback = true
			ret[i] = s.error(err)
		}

		ib := sqlbuilder.NewInsertBuilder()
		ib.InsertInto(s.table("transactions"))
		ib.Cols("id", "reference", "timestamp", "hash")
		ib.Values(t.ID, ref, t.Timestamp, t.Hash)

		sqlq, args := ib.BuildWithFlavor(s.flavor)
		_, err := tx.ExecContext(ctx, sqlq, args...)
		if err != nil {
			commitError(err)
			continue txLoop
		}

		for i, p := range t.Postings {
			ib := sqlbuilder.NewInsertBuilder()
			ib.InsertInto(s.table("postings"))
			ib.Cols("id", "txid", "source", "destination", "amount", "asset")
			ib.Values(i, t.ID, p.Source, p.Destination, p.Amount, p.Asset)

			sqlq, args := ib.BuildWithFlavor(s.flavor)

			_, err := tx.ExecContext(ctx, sqlq, args...)

			if err != nil {
				commitError(err)
				continue txLoop
			}
		}

		nextID, err := s.CountMeta(ctx)
		if err != nil {
			commitError(err)
			continue txLoop
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

			sqlq, args := ib.BuildWithFlavor(s.flavor)
			logrus.Debugln(sqlq, args)

			_, err = tx.ExecContext(ctx, sqlq, args...)
			if err != nil {
				commitError(err)
				continue txLoop
			}

			nextID++
		}
	}

	if mustRollback {
		err = tx.Rollback()
		if err != nil {
			return nil, err
		}
		return ret, storage.ErrAborted
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (s *Store) GetTransaction(ctx context.Context, txid string) (tx core.Transaction, err error) {
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
	sb.Where(sb.Equal("t.id", txid))
	sb.JoinWithOption(sqlbuilder.LeftJoin, sb.As(s.table("postings"), "p"), "p.txid = t.id")
	sb.OrderBy("p.id asc")

	sqlq, args := sb.BuildWithFlavor(s.flavor)

	rows, err := s.db.QueryContext(
		ctx,
		sqlq,
		args...,
	)

	if err != nil {
		return tx, s.error(err)
	}

	for rows.Next() {
		var txid int64
		var ts string
		var thash string
		var tref sql.NullString

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
		tx.Reference = tref.String

		tx.AppendPosting(posting)
	}

	meta, err := s.GetMeta(ctx, "transaction", fmt.Sprintf("%d", tx.ID))
	if err != nil {
		return tx, s.error(err)
	}
	tx.Metadata = meta

	return tx, nil
}

func (s *Store) LastTransaction(ctx context.Context) (*core.Transaction, error) {
	var lastTransaction core.Transaction

	q := query.New()
	q.Modify(query.Limit(1))

	c, err := s.FindTransactions(ctx, q)
	if err != nil {
		return nil, s.error(err)
	}

	txs := (c.Data).([]core.Transaction)
	if len(txs) > 0 {
		lastTransaction = txs[0]
		return &lastTransaction, nil
	}
	return nil, nil
}
