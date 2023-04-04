package ledger

import (
	"context"
	"math/big"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	storageerrors "github.com/formancehq/ledger/pkg/storage/sqlstorage/errors"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/pagination"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/lib/pq"
)

func (s *Store) GetBalancesAggregated(ctx context.Context, q storage.BalancesQuery) (core.AssetsBalances, error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "get_balances_aggregated")
	defer recordMetrics()

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
		return nil, storageerrors.PostgresError(err)
	}
	defer rows.Close()

	aggregatedBalances := core.AssetsBalances{}

	for rows.Next() {
		var (
			asset       string
			balancesStr string
		)
		if err = rows.Scan(&asset, &balancesStr); err != nil {
			return nil, storageerrors.PostgresError(err)
		}

		balances, ok := new(big.Int).SetString(balancesStr, 10)
		if !ok {
			panic("unable to restore big int")
		}

		aggregatedBalances[asset] = balances
	}
	if err := rows.Err(); err != nil {
		return nil, storageerrors.PostgresError(err)
	}

	return aggregatedBalances, nil
}

func (s *Store) GetBalances(ctx context.Context, q storage.BalancesQuery) (*api.Cursor[core.AccountsBalances], error) {
	if !s.isInitialized {
		return nil,
			storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "get_balances")
	defer recordMetrics()

	sb := s.schema.NewSelect(volumesTableName).
		Model((*Volumes)(nil)).
		ColumnExpr("account").
		ColumnExpr("array_agg((asset, input - output)) as arr").
		Group("account").
		Order("account DESC")

	if q.Filters.AfterAddress != "" {
		sb.Where("account < ?", q.Filters.AfterAddress)
	}

	if q.Filters.AddressRegexp != "" {
		sb.Where("account ~* ?", "^"+q.Filters.AddressRegexp+"$")
	}

	return pagination.UsingOffset(ctx, sb, storage.OffsetPaginatedQuery[storage.BalancesQueryFilters](q),
		func(accountsBalances *core.AccountsBalances, scanner interface{ Scan(args ...any) error }) error {
			var currentAccount string
			var arrayAgg []string
			if err := scanner.Scan(&currentAccount, pq.Array(&arrayAgg)); err != nil {
				return err
			}

			*accountsBalances = core.AccountsBalances{
				currentAccount: map[string]*big.Int{},
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
					return err
				}
				(*accountsBalances)[currentAccount][asset] = big.NewInt(balances)
			}

			return nil
		})
}
