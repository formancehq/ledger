package ledgerstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/formancehq/ledger/pkg/core"
	storageerrors "github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/api"
)

type BalancesQueryFilters struct {
	AfterAddress  string `json:"afterAddress"`
	AddressRegexp string `json:"addressRegexp"`
}

type BalancesQuery OffsetPaginatedQuery[BalancesQueryFilters]

func NewBalancesQuery() BalancesQuery {
	return BalancesQuery{
		PageSize: QueryDefaultPageSize,
		Order:    OrderAsc,
		Filters:  BalancesQueryFilters{},
	}
}

func (q BalancesQuery) GetPageSize() uint64 {
	return q.PageSize
}

func (b BalancesQuery) WithAfterAddress(after string) BalancesQuery {
	b.Filters.AfterAddress = after

	return b
}

func (b BalancesQuery) WithAddressFilter(address string) BalancesQuery {
	b.Filters.AddressRegexp = address

	return b
}

func (b BalancesQuery) WithPageSize(pageSize uint64) BalancesQuery {
	b.PageSize = pageSize
	return b
}

type balancesByAssets core.BalancesByAssets

func (b *balancesByAssets) Scan(value interface{}) error {
	var i sql.NullString
	if err := i.Scan(value); err != nil {
		return err
	}

	*b = balancesByAssets{}
	if err := json.Unmarshal([]byte(i.String), b); err != nil {
		return err
	}

	return nil
}

func (s *Store) GetBalancesAggregated(ctx context.Context, q BalancesQuery) (core.BalancesByAssets, error) {
	selectLastMoveForEachAccountAsset := s.schema.NewSelect(MovesTableName).
		ColumnExpr("account").
		ColumnExpr("asset").
		ColumnExpr(fmt.Sprintf(`"%s".first(post_commit_input_value order by timestamp desc) as post_commit_input_value`, s.schema.Name())).
		ColumnExpr(fmt.Sprintf(`"%s".first(post_commit_output_value order by timestamp desc) as post_commit_output_value`, s.schema.Name())).
		GroupExpr("account, asset")

	if q.Filters.AddressRegexp != "" {
		src := strings.Split(q.Filters.AddressRegexp, ":")
		selectLastMoveForEachAccountAsset.Where(fmt.Sprintf("jsonb_array_length(account_array) = %d", len(src)))

		for i, segment := range src {
			if segment == "" {
				continue
			}
			selectLastMoveForEachAccountAsset.Where(fmt.Sprintf("account_array @@ ('$[%d] == \"' || ?::text || '\"')::jsonpath", i), segment)
		}
	}

	type row struct {
		Asset      string `bun:"asset"`
		Aggregated *Int   `bun:"aggregated"`
	}

	rows := make([]row, 0)
	if err := s.schema.IDB.NewSelect().
		With("cte1", selectLastMoveForEachAccountAsset).
		Column("asset").
		ColumnExpr("sum(cte1.post_commit_input_value) - sum(cte1.post_commit_output_value) as aggregated").
		Table("cte1").
		Group("cte1.asset").
		Scan(ctx, &rows); err != nil {
		return nil, storageerrors.PostgresError(err)
	}

	aggregatedBalances := core.BalancesByAssets{}
	for _, row := range rows {
		aggregatedBalances[row.Asset] = (*big.Int)(row.Aggregated)
	}

	return aggregatedBalances, nil
}

func (s *Store) GetBalances(ctx context.Context, q BalancesQuery) (*api.Cursor[core.BalancesByAssetsByAccounts], error) {
	selectLastMoveForEachAccountAsset := s.schema.NewSelect(MovesTableName).
		ColumnExpr("account").
		ColumnExpr("asset").
		ColumnExpr(fmt.Sprintf(`"%s".first(post_commit_input_value order by timestamp desc) as post_commit_input_value`, s.schema.Name())).
		ColumnExpr(fmt.Sprintf(`"%s".first(post_commit_output_value order by timestamp desc) as post_commit_output_value`, s.schema.Name())).
		GroupExpr("account, asset")

	if q.Filters.AddressRegexp != "" {
		src := strings.Split(q.Filters.AddressRegexp, ":")
		selectLastMoveForEachAccountAsset.Where(fmt.Sprintf("jsonb_array_length(account_array) = %d", len(src)))

		for i, segment := range src {
			if len(segment) == 0 {
				continue
			}
			selectLastMoveForEachAccountAsset.Where(fmt.Sprintf("account_array @@ ('$[%d] == \"' || ?::text || '\"')::jsonpath", i), segment)
		}
	}

	if q.Filters.AfterAddress != "" {
		selectLastMoveForEachAccountAsset.Where("account > ?", q.Filters.AfterAddress)
	}

	query := s.schema.IDB.NewSelect().
		With("cte1", selectLastMoveForEachAccountAsset).
		Column("data.account").
		ColumnExpr(fmt.Sprintf(`"%s".aggregate_objects(data.asset) as balances_by_assets`, s.schema.Name())).
		TableExpr(`(
			select data.account, ('{"' || data.asset || '": ' || sum(data.post_commit_input_value) - sum(data.post_commit_output_value) || '}')::jsonb as asset
			from cte1 data
			group by data.account, data.asset
		) data`).
		Order("data.account").
		Group("data.account")

	type result struct {
		Account string           `bun:"account"`
		Assets  balancesByAssets `bun:"balances_by_assets"`
	}

	cursor, err := UsingOffset[BalancesQueryFilters, result](ctx,
		query, OffsetPaginatedQuery[BalancesQueryFilters](q))
	if err != nil {
		return nil, err
	}

	return api.MapCursor(cursor, func(from result) core.BalancesByAssetsByAccounts {
		return core.BalancesByAssetsByAccounts{
			from.Account: core.BalancesByAssets(from.Assets),
		}
	}), nil
}
