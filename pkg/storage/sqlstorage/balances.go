package sqlstorage

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/huandu/go-sqlbuilder"
	"github.com/lib/pq"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
)

func (s *Store) getBalances(ctx context.Context, exec executor, q storage.BalancesQuery) (sharedapi.Cursor[core.AccountsBalances], error) {
	sb := sqlbuilder.NewSelectBuilder()
	switch s.Schema().Flavor() {
	case sqlbuilder.PostgreSQL:
		sb.Select("account", "array_agg((asset, input - output))")
	case sqlbuilder.SQLite:
		// we try to get the same format as array_agg from postgres : {"(USD,-12686)","(EUR,-250)"}
		// so don't have to dev a marshal method for each storage
		sb.Select("account", `'{"(' || group_concat(asset||','||(input-output), ')","(')|| ')"}' as key_value_pairs`)
	}

	sb.From(s.schema.Table("volumes"))
	sb.GroupBy("account").OrderBy("account").Desc()

	t := BalancesPaginationToken{}

	if q.AfterAddress != "" {
		sb.Where(sb.L("account", q.AfterAddress))
		t.AfterAddress = q.AfterAddress
	}

	if q.Filters.Address != "" {
		arg := sb.Args.Add("^" + q.Filters.Address + "$")
		switch s.Schema().Flavor() {
		case sqlbuilder.PostgreSQL:
			sb.Where("account ~* " + arg)
		case sqlbuilder.SQLite:
			sb.Where("account REGEXP " + arg)
		}
		t.AddressRegexpFilter = q.Filters.Address
	}

	sb.Limit(int(q.Limit + 1))
	t.Limit = q.Limit
	sb.Offset(int(q.Offset))

	balanceQuery, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := exec.QueryContext(ctx, balanceQuery, args...)
	if err != nil {
		return sharedapi.Cursor[core.AccountsBalances]{}, s.error(err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			panic(err)
		}
	}(rows)

	res := make([]core.AccountsBalances, 0)

	for rows.Next() {
		var account string
		var arrayAgg []string
		if err = rows.Scan(&account, pq.Array(&arrayAgg)); err != nil {
			return sharedapi.Cursor[core.AccountsBalances]{}, s.error(err)
		}

		accountsBalances := core.AccountsBalances{
			account: core.AssetsBalances{},
		}

		// arrayAgg is in the form: []string{"(USD,-250)","(EUR,1000)"}
		for _, agg := range arrayAgg {
			// Remove parenthesis
			agg = agg[1 : len(agg)-1]
			// Split the asset and balances on the comma separator
			split := strings.Split(agg, ",")
			asset := split[0]
			balancesString := split[1]
			balances, err := strconv.ParseInt(balancesString, 10, 64)
			if err != nil {
				return sharedapi.Cursor[core.AccountsBalances]{}, s.error(err)
			}
			accountsBalances[account][asset] = balances
		}

		res = append(res, accountsBalances)
	}

	if err := rows.Err(); err != nil {
		return sharedapi.Cursor[core.AccountsBalances]{}, s.error(err)
	}

	var previous, next string
	if int(q.Offset)-int(q.Limit) >= 0 {
		t.Offset = q.Offset - q.Limit
		raw, err := json.Marshal(t)
		if err != nil {
			return sharedapi.Cursor[core.AccountsBalances]{}, s.error(err)
		}
		previous = base64.RawURLEncoding.EncodeToString(raw)
	}

	if len(res) == int(q.Limit+1) {
		res = res[:len(res)-1]
		t.Offset = q.Offset + q.Limit
		raw, err := json.Marshal(t)
		if err != nil {
			return sharedapi.Cursor[core.AccountsBalances]{}, s.error(err)
		}
		next = base64.RawURLEncoding.EncodeToString(raw)
	}

	return sharedapi.Cursor[core.AccountsBalances]{
		PageSize: len(res),
		HasMore:  next != "",
		Previous: previous,
		Next:     next,
		Data:     res,
	}, nil
}

func (s *Store) getBalancesAggregated(ctx context.Context, exec executor, q storage.BalancesQuery) (core.AssetsBalances, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("asset", "sum(input - output)")
	sb.From(s.schema.Table("volumes"))
	sb.GroupBy("asset")

	if q.Filters.Address != "" {
		arg := sb.Args.Add("^" + q.Filters.Address + "$")
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
		if err := rows.Close(); err != nil {
			panic(err)
		}
	}(rows)

	res := core.AssetsBalances{}

	for rows.Next() {
		var (
			asset    string
			balances int64
		)
		if err = rows.Scan(&asset, &balances); err != nil {
			return nil, s.error(err)
		}

		res[asset] = balances
	}
	if err := rows.Err(); err != nil {
		return nil, s.error(err)
	}

	return res, nil
}
