package ledger

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	sqlerrors "github.com/formancehq/ledger/pkg/storage/sqlstorage/errors"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/lib/pq"
)

type BalancesPaginationToken struct {
	PageSize            uint   `json:"pageSize"`
	Offset              uint   `json:"offset"`
	AfterAddress        string `json:"after,omitempty"`
	AddressRegexpFilter string `json:"address,omitempty"`
}

func (s *Store) GetBalancesAggregated(ctx context.Context, q storage.BalancesQuery) (core.AssetsBalances, error) {
	if !s.isInitialized {
		return nil, storage.ErrStoreNotInitialized
	}

	sb := s.schema.NewSelect(volumesTableName).
		Model((*Volumes)(nil)).
		ColumnExpr("asset").
		ColumnExpr("sum(input - output) as arr").
		Group("asset")

	if q.Filters.AddressRegexp != "" {
		sb.Where("account ~* ?", "^"+q.Filters.AddressRegexp+"$")
	}

	rows, err := s.schema.QueryContext(ctx, sb.String())
	if err != nil {
		return nil, sqlerrors.PostgresError(err)
	}
	defer rows.Close()

	aggregatedBalances := core.AssetsBalances{}

	for rows.Next() {
		var (
			asset       string
			balancesStr string
		)
		if err = rows.Scan(&asset, &balancesStr); err != nil {
			return nil, sqlerrors.PostgresError(err)
		}

		balances, ok := new(big.Int).SetString(balancesStr, 10)
		if !ok {
			panic("unable to restore big int")
		}

		aggregatedBalances[asset] = balances
	}
	if err := rows.Err(); err != nil {
		return nil, sqlerrors.PostgresError(err)
	}

	return aggregatedBalances, nil
}

func (s *Store) GetBalances(ctx context.Context, q storage.BalancesQuery) (api.Cursor[core.AccountsBalances], error) {
	if !s.isInitialized {
		return api.Cursor[core.AccountsBalances]{}, storage.ErrStoreNotInitialized
	}

	sb := s.schema.NewSelect(volumesTableName).
		Model((*Volumes)(nil)).
		ColumnExpr("account").
		ColumnExpr("array_agg((asset, input - output)) as arr").
		Group("account").
		Order("account DESC")

	t := BalancesPaginationToken{}

	if q.AfterAddress != "" {
		sb.Where("account < ?", q.AfterAddress)
		t.AfterAddress = q.AfterAddress
	}

	if q.Filters.AddressRegexp != "" {
		sb.Where("account ~* ?", "^"+q.Filters.AddressRegexp+"$")
		t.AddressRegexpFilter = q.Filters.AddressRegexp
	}

	sb.Limit(int(q.PageSize + 1))
	t.PageSize = q.PageSize
	sb.Offset(int(q.Offset))

	rows, err := s.schema.QueryContext(ctx, sb.String())
	if err != nil {
		return api.Cursor[core.AccountsBalances]{}, sqlerrors.PostgresError(err)
	}
	defer rows.Close()

	accounts := make([]core.AccountsBalances, 0)

	for rows.Next() {
		var currentAccount string
		var arrayAgg []string
		if err = rows.Scan(&currentAccount, pq.Array(&arrayAgg)); err != nil {
			return api.Cursor[core.AccountsBalances]{}, sqlerrors.PostgresError(err)
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
				return api.Cursor[core.AccountsBalances]{}, sqlerrors.PostgresError(err)
			}
			accountsBalances[currentAccount][asset] = big.NewInt(balances)
		}

		accounts = append(accounts, accountsBalances)
	}

	if err := rows.Err(); err != nil {
		return api.Cursor[core.AccountsBalances]{}, sqlerrors.PostgresError(err)
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
			return api.Cursor[core.AccountsBalances]{}, sqlerrors.PostgresError(err)
		}
		previous = base64.RawURLEncoding.EncodeToString(raw)
	}

	if len(accounts) == int(q.PageSize+1) {
		accounts = accounts[:len(accounts)-1]
		t.Offset = q.Offset + q.PageSize
		raw, err := json.Marshal(t)
		if err != nil {
			return api.Cursor[core.AccountsBalances]{}, sqlerrors.PostgresError(err)
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
