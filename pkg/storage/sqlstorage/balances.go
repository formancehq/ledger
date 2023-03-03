package sqlstorage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/huandu/go-sqlbuilder"
	"github.com/lib/pq"
)

func (s *Store) GetBalancesAggregated(ctx context.Context, q ledger.BalancesQuery) (core.AssetsBalances, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("asset", "sum(input - output)")
	sb.From(s.schema.Table("volumes"))
	sb.GroupBy("asset")

	if q.Filters.AddressRegexp != "" {
		arg := sb.Args.Add("^" + q.Filters.AddressRegexp + "$")
		sb.Where("account ~* " + arg)
	}

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return nil, err
	}

	balanceAggregatedQuery, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := executor.QueryContext(ctx, balanceAggregatedQuery, args...)
	if err != nil {
		return nil, s.error(err)
	}
	defer rows.Close()

	aggregatedBalances := core.AssetsBalances{}

	for rows.Next() {
		var (
			asset       string
			balancesStr string
		)
		if err = rows.Scan(&asset, &balancesStr); err != nil {
			return nil, s.error(err)
		}

		balances, err := core.ParseMonetaryInt(balancesStr)

		if err != nil {
			return nil, s.error(err)
		}

		aggregatedBalances[asset] = balances
	}
	if err := rows.Err(); err != nil {
		return nil, s.error(err)
	}

	return aggregatedBalances, nil
}

func (s *Store) GetBalances(ctx context.Context, q ledger.BalancesQuery) (api.Cursor[core.AccountsBalances], error) {
	executor, err := s.executorProvider(ctx)
	if err != nil {
		return api.Cursor[core.AccountsBalances]{}, err
	}

	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("account", "array_agg((asset, input - output))")

	sb.From(s.schema.Table("volumes"))
	sb.GroupBy("account")
	sb.OrderBy("account desc")

	t := BalancesPaginationToken{}

	if q.AfterAddress != "" {
		sb.Where(sb.L("account", q.AfterAddress))
		t.AfterAddress = q.AfterAddress
	}

	if q.Filters.AddressRegexp != "" {
		arg := sb.Args.Add("^" + q.Filters.AddressRegexp + "$")
		sb.Where("account ~* " + arg)
		t.AddressRegexpFilter = q.Filters.AddressRegexp
	}

	sb.Limit(int(q.PageSize + 1))
	t.PageSize = q.PageSize
	sb.Offset(int(q.Offset))

	balanceQuery, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := executor.QueryContext(ctx, balanceQuery, args...)
	if err != nil {
		return api.Cursor[core.AccountsBalances]{}, s.error(err)
	}
	defer rows.Close()

	accounts := make([]core.AccountsBalances, 0)

	for rows.Next() {
		var currentAccount string
		var arrayAgg []string
		if err = rows.Scan(&currentAccount, pq.Array(&arrayAgg)); err != nil {
			return api.Cursor[core.AccountsBalances]{}, s.error(err)
		}

		accountsBalances := core.AccountsBalances{
			currentAccount: core.AssetsBalances{},
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
				return api.Cursor[core.AccountsBalances]{}, s.error(err)
			}
			accountsBalances[currentAccount][asset] = core.NewMonetaryInt(balances)
		}

		accounts = append(accounts, accountsBalances)
	}

	if err := rows.Err(); err != nil {
		return api.Cursor[core.AccountsBalances]{}, s.error(err)
	}

	var previous, next string
	if q.Offset > 0 {
		offset := int(q.Offset) - int(q.PageSize)
		if offset < 0 {
			t.Offset = 0
		} else {
			t.Offset = uint(offset)
		}
		raw, err := json.Marshal(t)
		if err != nil {
			return api.Cursor[core.AccountsBalances]{}, s.error(err)
		}
		previous = base64.RawURLEncoding.EncodeToString(raw)
	}

	if len(accounts) == int(q.PageSize+1) {
		accounts = accounts[:len(accounts)-1]
		t.Offset = q.Offset + q.PageSize
		raw, err := json.Marshal(t)
		if err != nil {
			return api.Cursor[core.AccountsBalances]{}, s.error(err)
		}
		next = base64.RawURLEncoding.EncodeToString(raw)
	}

	hasMore := next != ""
	return api.Cursor[core.AccountsBalances]{
		PageSize: int(q.PageSize),
		HasMore:  hasMore,
		Previous: previous,
		Next:     next,
		Data:     accounts,
	}, nil
}
