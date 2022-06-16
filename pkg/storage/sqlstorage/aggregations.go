package sqlstorage

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/numary/go-libs/sharedapi"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
)

func (s *Store) countTransactions(ctx context.Context, exec executor, tq storage.TransactionsQuery) (uint64, error) {
	var count uint64

	sb, _ := s.buildTransactionsQuery(tq)
	q, args := sb.BuildWithFlavor(s.schema.Flavor())
	q = fmt.Sprintf(`SELECT count(*) FROM (%s) AS t`, q)
	err := exec.QueryRowContext(ctx, q, args...).Scan(&count)

	return count, s.error(err)
}

func (s *Store) CountTransactions(ctx context.Context, q storage.TransactionsQuery) (uint64, error) {
	return s.countTransactions(ctx, s.schema, q)
}

func (s *Store) countAccounts(ctx context.Context, exec executor, q storage.AccountsQuery) (uint64, error) {
	var count uint64

	sb, _ := s.buildAccountsQuery(q)
	sqlq, args := sb.Select("count(*)").BuildWithFlavor(s.schema.Flavor())
	err := exec.QueryRowContext(ctx, sqlq, args...).Scan(&count)

	return count, s.error(err)
}

func (s *Store) CountAccounts(ctx context.Context, q storage.AccountsQuery) (uint64, error) {
	return s.countAccounts(ctx, s.schema, q)
}

func (s *Store) getAssetsVolumes(ctx context.Context, exec executor, accountAddress string) (core.AssetsVolumes, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("asset", "input", "output")
	sb.From(s.schema.Table("volumes"))
	sb.Where(sb.E("account", accountAddress))

	q, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := exec.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, s.error(err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			panic(err)
		}
	}(rows)

	volumes := core.AssetsVolumes{}
	for rows.Next() {
		var (
			asset  string
			input  int64
			output int64
		)
		err = rows.Scan(&asset, &input, &output)
		if err != nil {
			return nil, s.error(err)
		}
		volumes[asset] = core.Volumes{
			Input:  input,
			Output: output,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, s.error(err)
	}

	return volumes, nil
}

func (s *Store) GetAssetsVolumes(ctx context.Context, accountAddress string) (core.AssetsVolumes, error) {
	return s.getAssetsVolumes(ctx, s.schema, accountAddress)
}

func (s *Store) getVolumes(ctx context.Context, exec executor, accountAddress, asset string) (core.Volumes, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("input", "output")
	sb.From(s.schema.Table("volumes"))
	sb.Where(sb.And(sb.E("account", accountAddress), sb.E("asset", asset)))

	q, args := sb.BuildWithFlavor(s.schema.Flavor())
	row := exec.QueryRowContext(ctx, q, args...)
	if row.Err() != nil {
		return core.Volumes{}, s.error(row.Err())
	}

	var input, output int64
	if err := row.Scan(&input, &output); err != nil {
		if err == sql.ErrNoRows {
			return core.Volumes{}, nil
		}
		return core.Volumes{}, s.error(err)
	}

	return core.Volumes{
		Input:  input,
		Output: output,
	}, nil
}

func (s *Store) GetVolumes(ctx context.Context, accountAddress, asset string) (core.Volumes, error) {
	return s.getVolumes(ctx, s.schema, accountAddress, asset)
}

func (s *Store) getAggregatedBalances(ctx context.Context, exec executor, q storage.BalancesQuery) (sharedapi.Cursor[core.AggregatedBalances], error) {
	result := make([]core.AggregatedBalances, 0)
	// ex: "aggregated":{"USD": 50,"EUR": 225},{"account1":{"EUR":25,"USD":50},"account2":{"EUR":200}}

	aggregatedBalances, err := s.GetAggregatedBalancesData(ctx, exec, q)
	if err != nil {
		return sharedapi.Cursor[core.AggregatedBalances]{}, err
	}

	accountsBalances, err := s.GetBalancesAccountsData(ctx, exec, q)
	if err != nil {
		return sharedapi.Cursor[core.AggregatedBalances]{}, err
	}

	result[0].Aggregated = aggregatedBalances
	result[0].Accounts = accountsBalances

	var previous, next string

	return sharedapi.Cursor[core.AggregatedBalances]{
		PageSize: len(accountsBalances),
		Previous: previous,
		Next:     next,
		Data:     result,
	}, nil
}

func (s *Store) GetAggregatedBalances(ctx context.Context, q storage.BalancesQuery) (sharedapi.Cursor[core.AggregatedBalances], error) {
	return s.getAggregatedBalances(ctx, s.schema, q)
}
