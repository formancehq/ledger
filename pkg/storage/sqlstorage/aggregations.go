package sqlstorage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
)

func (s *Store) countTransactions(ctx context.Context, exec executor, params map[string]interface{}) (uint64, error) {
	var count uint64

	sb := s.buildTransactionsQuery(params)
	q, args := sb.BuildWithFlavor(s.schema.Flavor())
	q = fmt.Sprintf(`SELECT count(*) FROM (%s) AS t`, q)
	err := exec.QueryRowContext(ctx, q, args...).Scan(&count)

	return count, s.error(err)
}

func (s *Store) CountTransactions(ctx context.Context, q query.Transactions) (uint64, error) {
	return s.countTransactions(ctx, s.schema, q.Params)
}

func (s *Store) countAccounts(ctx context.Context, exec executor, p map[string]interface{}) (uint64, error) {
	var count uint64

	sb := s.buildAccountsQuery(p)
	sqlq, args := sb.Select("count(*)").BuildWithFlavor(s.schema.Flavor())
	err := exec.QueryRowContext(ctx, sqlq, args...).Scan(&count)

	return count, s.error(err)
}

func (s *Store) CountAccounts(ctx context.Context, q query.Accounts) (uint64, error) {
	return s.countAccounts(ctx, s.schema, q.Params)
}

func (s *Store) aggregateVolumes(ctx context.Context, exec executor, address string) (core.Volumes, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("asset", "input", "output")
	sb.From(s.schema.Table("volumes"))
	sb.Where(sb.E("account", address))

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

	volumes := make(map[string]map[string]int64)
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
		volumes[asset] = map[string]int64{
			"input":  input,
			"output": output,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, s.error(err)
	}

	return volumes, nil
}

func (s *Store) AggregateVolumes(ctx context.Context, address string) (core.Volumes, error) {
	return s.aggregateVolumes(ctx, s.schema, address)
}
