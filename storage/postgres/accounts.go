package postgres

import (
	"context"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger/query"
)

func (s *PGStore) CountAccounts() (int64, error) {
	var count int64

	sqlq, _ := sqlbuilder.
		Select("count(*)").
		From(s.table("addresses")).
		BuildWithFlavor(sqlbuilder.PostgreSQL)

	err := s.Conn().QueryRow(
		context.Background(),
		sqlq,
	).Scan(&count)

	return count, err
}

func (s *PGStore) FindAccounts(q query.Query) (query.Cursor, error) {
	c := query.Cursor{}
	results := []core.Account{}

	queryRem := sqlbuilder.Select("count(*)")
	queryRem.From(s.table("addresses"))

	if q.After != "" {
		queryRem.Where(queryRem.LessThan("address", q.After))
	}

	sqlRem, args := queryRem.BuildWithFlavor(sqlbuilder.PostgreSQL)

	var remaining int

	err := s.Conn().QueryRow(
		context.Background(),
		sqlRem,
		args...,
	).Scan(&remaining)

	if err != nil {
		return c, err
	}

	queryAcc := sqlbuilder.
		Select("address").
		From(s.table("addresses")).
		GroupBy("address").
		OrderBy("address desc").
		Limit(q.Limit)

	if q.After != "" {
		queryAcc.Where(queryAcc.LessThan("address", q.After))
	}

	if q.HasParam("meta_key") {
		queryAcc.JoinWithOption(
			sqlbuilder.LeftJoin,
			queryAcc.As("metadata", "m"),
			"m.meta_target_id = address",
		)
		queryAcc.Where(
			queryAcc.Equal("m.meta_key", q.Params["meta_key"]),
			queryAcc.Equal("m.meta_target_type", "account"),
		)
		if q.HasParam("meta_value") {
			queryAcc.Where(
				queryAcc.Equal("m.meta_value", q.Params["meta_value"]),
			)
		}
	}

	sqlAcc, args := queryAcc.BuildWithFlavor(sqlbuilder.PostgreSQL)

	rows, err := s.Conn().Query(
		context.Background(),
		sqlAcc,
		args...,
	)

	if err != nil {
		return c, err
	}

	defer rows.Close()

	for rows.Next() {
		var address string

		err := rows.Scan(&address)

		if err != nil {
			return c, err
		}

		account := core.Account{
			Address:  address,
			Contract: "default",
		}

		meta, err := s.GetMeta("account", account.Address)
		if err != nil {
			return c, err
		}
		account.Metadata = meta

		results = append(results, account)
	}

	total, _ := s.CountAccounts()

	c.PageSize = q.Limit
	c.HasMore = len(results) < remaining
	c.Remaining = remaining - len(results)
	c.Total = total
	c.Data = results

	return c, nil
}
