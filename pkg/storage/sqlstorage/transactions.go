package sqlstorage

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg/storage"
	"math"
	"sort"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
)

func (s *Store) findTransactions(ctx context.Context, exec executor, q query.Query) (sharedapi.Cursor, error) {
	q.Limit = int(math.Max(-1, math.Min(float64(q.Limit), 100))) + 1

	c := sharedapi.Cursor{}
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

	rows, err := exec.QueryContext(ctx, sqlq, args...)
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
				ID: txid,
				TransactionData: core.TransactionData{
					Postings:  []core.Posting{},
					Reference: ref.String,
					Metadata:  core.Metadata{},
				},
				Timestamp: ts,
				Hash:      thash,
			}
		}

		t := transactions[txid]
		t.AppendPosting(posting)
		transactions[txid] = t
	}
	if rows.Err() != nil {
		return sharedapi.Cursor{}, s.error(rows.Err())
	}

	for _, t := range transactions {
		meta, err := s.getMeta(ctx, exec, "transaction", fmt.Sprintf("%d", t.ID))
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

	total, err := s.countTransactions(ctx, exec)
	if err != nil {
		return c, err
	}
	c.Total = total

	return c, nil
}

func (s *Store) FindTransactions(ctx context.Context, q query.Query) (sharedapi.Cursor, error) {
	return s.findTransactions(ctx, s.db, q)
}

func (s *Store) saveTransactions(ctx context.Context, exec executor, ts []core.Transaction) (map[int]error, error) {

	ret := make(map[int]error)
	hasError := false

txLoop:
	for i, t := range ts {
		var ref *string

		if t.Reference != "" {
			ref = &t.Reference
		}

		commitError := func(err error) {
			ret[i] = s.error(err)
			hasError = true
		}

		ib := sqlbuilder.NewInsertBuilder()
		ib.InsertInto(s.table("transactions"))
		ib.Cols("id", "reference", "timestamp", "hash")
		ib.Values(t.ID, ref, t.Timestamp, t.Hash)

		sqlq, args := ib.BuildWithFlavor(s.flavor)
		_, err := exec.ExecContext(ctx, sqlq, args...)
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
			_, err := exec.ExecContext(ctx, sqlq, args...)

			if err != nil {
				commitError(err)
				continue txLoop
			}
		}

		nextID, err := s.countMeta(ctx, exec)
		if err != nil {
			commitError(err)
			continue txLoop
		}

		for key, value := range t.Metadata {
			ib := sqlbuilder.NewInsertBuilder()
			ib.InsertInto(s.table("metadata"))
			ib.Cols("meta_id", "meta_target_type", "meta_target_id", "meta_key", "meta_value", "timestamp")
			ib.Values(int(nextID), "transaction", fmt.Sprintf("%d", t.ID), key, string(value), t.Timestamp)

			sqlq, args := ib.BuildWithFlavor(s.flavor)
			_, err = exec.ExecContext(ctx, sqlq, args...)
			if err != nil {
				commitError(err)
				continue txLoop
			}

			nextID++
		}
	}

	if hasError {
		return ret, storage.ErrAborted
	}

	return ret, nil
}

func (s *Store) SaveTransactions(ctx context.Context, ts []core.Transaction) (map[int]error, error) {

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, s.error(err)
	}

	ret, err := s.saveTransactions(ctx, tx, ts)
	if err != nil {
		tx.Rollback()
		return ret, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (s *Store) getTransaction(ctx context.Context, exec executor, txid string) (tx core.Transaction, err error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("t.id", "t.timestamp", "t.hash", "t.reference", "p.source", "p.destination", "p.amount", "p.asset")
	sb.From(sb.As(s.table("transactions"), "t"))
	sb.Where(sb.Equal("t.id", txid))
	sb.JoinWithOption(sqlbuilder.LeftJoin, sb.As(s.table("postings"), "p"), "p.txid = t.id")
	sb.OrderBy("p.id asc")

	sqlq, args := sb.BuildWithFlavor(s.flavor)
	rows, err := exec.QueryContext(ctx, sqlq, args...)
	if err != nil {
		return tx, s.error(err)
	}

	for rows.Next() {
		var txId int64
		var ts string
		var txHash string
		var txRef sql.NullString

		posting := core.Posting{}

		err := rows.Scan(
			&txId,
			&ts,
			&txHash,
			&txRef,
			&posting.Source,
			&posting.Destination,
			&posting.Amount,
			&posting.Asset,
		)
		if err != nil {
			return tx, err
		}

		tx.ID = txId
		tx.Timestamp = ts
		tx.Hash = txHash
		tx.Metadata = core.Metadata{}
		tx.Reference = txRef.String

		tx.AppendPosting(posting)
	}
	if rows.Err() != nil {
		return tx, s.error(rows.Err())
	}

	meta, err := s.getMeta(ctx, exec, "transaction", fmt.Sprintf("%d", tx.ID))
	if err != nil {
		return tx, err
	}
	tx.Metadata = meta

	return tx, nil
}

func (s *Store) GetTransaction(ctx context.Context, txId string) (tx core.Transaction, err error) {
	return s.getTransaction(ctx, s.db, txId)
}

func (s *Store) lastTransaction(ctx context.Context, exec executor) (*core.Transaction, error) {
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
	sb.JoinWithOption(sqlbuilder.LeftJoin, sb.As(s.table("postings"), "p"), "p.txid = t.id")
	sb.SQL(fmt.Sprintf("WHERE t.id = (select count(*) from %s) - 1", s.table("transactions")))

	sqlq, args := sb.BuildWithFlavor(s.flavor)
	sharedlogging.GetLogger(ctx).Debug(sqlq)
	rows, err := exec.QueryContext(ctx, sqlq, args...)
	if err != nil {
		return nil, s.error(err)
	}

	tx := core.Transaction{}

	for rows.Next() {
		var ref sql.NullString
		posting := core.Posting{}
		err := rows.Scan(
			&tx.ID,
			&tx.Timestamp,
			&tx.Hash,
			&ref,
			&posting.Source,
			&posting.Destination,
			&posting.Amount,
			&posting.Asset,
		)
		if err != nil {
			return nil, s.error(err)
		}
		tx.Reference = ref.String
		tx.AppendPosting(posting)
	}
	if rows.Err() != nil {
		return nil, s.error(rows.Err())
	}

	if len(tx.Postings) == 0 {
		return nil, nil
	}

	meta, err := s.getMeta(ctx, exec, "transaction", fmt.Sprintf("%d", tx.ID))
	if err != nil {
		return nil, err
	}
	tx.Metadata = meta

	return &tx, nil
}

func (s *Store) LastTransaction(ctx context.Context) (*core.Transaction, error) {
	return s.lastTransaction(ctx, s.db)
}
