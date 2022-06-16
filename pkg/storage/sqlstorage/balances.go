package sqlstorage

import (
	"context"
	"database/sql"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
)

func (s *Store) GetBalancesAccountsData(ctx context.Context, exec executor, q storage.BalancesQuery) (core.AccountsBalances, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("account", "asset", "input - output as balance")
	sb.From(s.schema.Table("volumes"))
	sb.GroupBy("account", "asset", "balance")

	if q.AfterAddress != "" {
		sb.Where(sb.L("address", q.AfterAddress))
	}

	if q.Params.Account != "" {
		arg := sb.Args.Add("^" + q.Params.Account + "$")
		switch s.Schema().Flavor() {
		case sqlbuilder.PostgreSQL:
			sb.Where("account ~* " + arg)
		case sqlbuilder.SQLite:
			sb.Where("account REGEXP " + arg)
		}
	}

	balanceQuery, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := exec.QueryContext(ctx, balanceQuery, args...)
	if err != nil {
		return nil, s.error(err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			panic(err)
		}
	}(rows)

	var accounts core.AccountsBalances

	for rows.Next() {
		var (
			currentAccount string
			asset          string
			balance        int64
		)
		err = rows.Scan(&currentAccount, &asset, &balance)
		if err != nil {
			return nil, s.error(err)
		}

		// if the accounts already exists in the map, we simply want to add an asset, not to override the last map
		if _, exists := accounts[currentAccount]; exists {
			accounts[currentAccount][asset] = balance
		} else {
			accounts[currentAccount] = map[string]int64{
				asset: balance,
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, s.error(err)
	}

	return accounts, nil
}

func (s *Store) GetAggregatedBalancesData(ctx context.Context, exec executor, q storage.BalancesQuery) (map[string]int64, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("asset", "sum(input - output)")
	sb.From(s.schema.Table("volumes"))
	sb.GroupBy("asset")

	if q.Params.Account != "" {
		arg := sb.Args.Add("^" + q.Params.Account + "$")
		switch s.Schema().Flavor() {
		case sqlbuilder.PostgreSQL:
			sb.Where("account ~* " + arg)
		case sqlbuilder.SQLite:
			sb.Where("account REGEXP " + arg)
		}
	}

	balanceAggregatedQuery, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := exec.QueryContext(ctx, balanceAggregatedQuery, args...)
	if err != nil {
		return nil, s.error(err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			panic(err)
		}
	}(rows)

	aggregatedBalances := make(map[string]int64)

	for rows.Next() {
		var (
			asset  string
			amount int64
		)
		err = rows.Scan(&asset, &amount)
		if err != nil {
			return nil, s.error(err)
		}

		aggregatedBalances[asset] = amount

	}

	if err := rows.Err(); err != nil {
		return nil, s.error(err)
	}

	return aggregatedBalances, nil
}
