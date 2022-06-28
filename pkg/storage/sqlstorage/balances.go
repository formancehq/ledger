package sqlstorage

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
)

func (s *Store) GetBalancesAccountsData(ctx context.Context, exec executor, q storage.BalancesQuery) (sharedapi.Cursor[core.AccountsBalances], error) {
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
	sb.GroupBy("account")

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
		err := rows.Close()
		if err != nil {
			panic(err)
		}
	}(rows)

	accounts := make([]core.AccountsBalances, 0)

	for rows.Next() {
		var (
			currentAccount string

			assetBalance []byte
		)
		err = rows.Scan(&currentAccount, &assetBalance)
		if err != nil {
			return sharedapi.Cursor[core.AccountsBalances]{}, s.error(err)
		}

		// we (clean and) marshal the computed row in a map[string]int64
		assetBalances := arrayToAssetBalance(assetBalance)
		if err != nil {
			return sharedapi.Cursor[core.AccountsBalances]{}, s.error(err)
		}

		accounts = append(accounts, map[string]map[string]int64{
			currentAccount: assetBalances,
		})
	}

	if err := rows.Err(); err != nil {
		return sharedapi.Cursor[core.AccountsBalances]{}, s.error(err)
	}

	var previous, next string
	if q.Offset > 0 {
		offset := int(q.Offset) - int(q.Limit)
		if offset < 0 {
			t.Offset = 0
		} else {
			t.Offset = uint(offset)
		}
		raw, err := json.Marshal(t)
		if err != nil {
			return sharedapi.Cursor[core.AccountsBalances]{}, s.error(err)
		}
		previous = base64.RawURLEncoding.EncodeToString(raw)
	}

	if len(accounts) == int(q.Limit+1) {
		accounts = accounts[:len(accounts)-1]
		t.Offset = q.Offset + q.Limit
		raw, err := json.Marshal(t)
		if err != nil {
			return sharedapi.Cursor[core.AccountsBalances]{}, s.error(err)
		}
		next = base64.RawURLEncoding.EncodeToString(raw)
	}

	return sharedapi.Cursor[core.AccountsBalances]{
		PageSize: len(accounts),
		Previous: previous,
		Next:     next,
		Data:     accounts,
	}, nil
}

func (s *Store) GetAggregatedBalancesData(ctx context.Context, exec executor, q storage.BalancesQuery) (map[string]int64, error) {
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

func arrayToAssetBalance(array []byte) map[string]int64 {
	if len(array) <= 3 {
		return nil
	}

	splitRegex := regexp.MustCompile(`([^,]+,[^,]+)`)
	arrayBalances := splitRegex.FindAllString(string(array), -1)

	result := make(map[string]int64)

	for i, assetBalance := range arrayBalances {
		values := strings.Split(assetBalance, ",")

		var balance int64
		if _, err := fmt.Sscanf(values[1], "%d", &balance); err != nil {
			panic(errors.Wrap(err, "error while converting balance value into map"))
		}

		// this is because the format we get from pg with array_agg is {"(USD,-12686)","(EUR,-250)"}
		// and we have {"(  to remove for the first split and "( for the second split
		// trimming the string could be dangerous (assets are not sanitized)
		if i == 0 {
			result[values[0][3:]] = balance
		} else {
			result[values[0][2:]] = balance
		}
	}

	return result
}
