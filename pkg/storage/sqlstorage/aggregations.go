package sqlstorage

import (
	"context"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
)

func (s *Store) countTransactions(ctx context.Context, exec executor) (int64, error) {
	var count int64

	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("count(*)")
	sb.From(s.schema.Table("transactions"))

	sqlq, args := sb.Build()

	err := exec.QueryRowContext(ctx, sqlq, args...).Scan(&count)

	return count, s.error(err)
}

func (s *Store) CountTransactions(ctx context.Context) (int64, error) {
	return s.countTransactions(ctx, s.schema)
}

func (s *Store) countAccounts(ctx context.Context, exec executor) (int64, error) {
	var count int64

	sb := sqlbuilder.NewSelectBuilder()
	sb.
		Select("count(*)").
		From(s.schema.Table("accounts")).
		BuildWithFlavor(s.schema.Flavor())

	sqlq, args := sb.Build()

	err := exec.QueryRowContext(ctx, sqlq, args...).Scan(&count)

	return count, s.error(err)
}

func (s *Store) CountAccounts(ctx context.Context) (int64, error) {
	return s.countAccounts(ctx, s.schema)
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
