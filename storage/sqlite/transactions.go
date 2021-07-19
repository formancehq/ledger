package sqlite

import (
	"fmt"
	"math"
	"sort"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger/query"
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

	sb := sqlbuilder.NewSelectBuilder()
	sb.Select(
		"t.id",
		"t.timestamp",
		"t.hash",
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
	fmt.Println(sqlq, args)

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

		posting := core.Posting{}

		rows.Scan(
			&txid,
			&ts,
			&thash,
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

	c.PageSize = q.Limit - 1

	c.HasMore = len(results) == q.Limit
	if c.HasMore {
		results = results[:len(results)-1]
	}
	c.Data = results

	total, _ := s.CountTransactions()
	c.Total = int(total)

	return c, nil
}
