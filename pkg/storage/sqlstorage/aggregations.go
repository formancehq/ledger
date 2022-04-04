package sqlstorage

import (
	"context"
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
)

func (s *Store) countTransactions(ctx context.Context, exec executor, params map[string]interface{}) (int64, error) {
	var count int64

	tq := s.transactionsQuery(params)
	sqlq, args := tq.BuildWithFlavor(s.schema.Flavor())
	query := fmt.Sprintf(`SELECT count(*) FROM (%s) AS t`, sqlq)

	err := exec.QueryRowContext(ctx, query, args...).Scan(&count)

	return count, s.error(err)
}

func (s *Store) CountTransactions(ctx context.Context) (int64, error) {
	return s.countTransactions(ctx, s.schema, map[string]interface{}{})
}

func (s *Store) countAccounts(ctx context.Context, exec executor, p map[string]interface{}) (int64, error) {
	var count int64

	sqlq, args := s.accountsQuery(p).Select("count(*)").BuildWithFlavor(s.schema.Flavor())
	err := exec.QueryRowContext(ctx, sqlq, args...).Scan(&count)

	return count, s.error(err)
}

func (s *Store) CountAccounts(ctx context.Context) (int64, error) {
	return s.countAccounts(ctx, s.schema, map[string]interface{}{})
}

func (s *Store) aggregateVolumes(ctx context.Context, exec executor, address string) (core.Volumes, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("asset", "input", "output")
	sb.From(s.schema.Table("volumes"))
	sb.Where(sb.E("account", address))

	sql, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := exec.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, s.error(err)
	}
	defer rows.Close()

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
