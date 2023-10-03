package sqlstorage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/patrickmn/go-cache"
)

func (s *Store) GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	account, ok := s.cache.Get(address)
	// When having a single instance of the ledger, we can use the cached account.
	// Otherwise, compute it every single time for now.
	if s.singleWriter && ok {
		return account.(*core.AccountWithVolumes).Copy(), nil
	}

	acc := core.Account{
		Address:  core.AccountAddress(address),
		Metadata: core.Metadata{},
	}
	assetsVolumes := core.AssetsVolumes{}

	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("accounts.metadata", "volumes.asset", "volumes.input", "volumes.output")
	sb.From(s.schema.Table("accounts"))
	sb.JoinWithOption(sqlbuilder.LeftOuterJoin,
		s.schema.Table("volumes"),
		"accounts.address = volumes.account")
	sb.Where(sb.E("accounts.address", address))

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return nil, err
	}

	q, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := executor.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, s.error(err)
	}
	defer rows.Close()

	for rows.Next() {
		var asset, inputStr, outputStr sql.NullString
		if err := rows.Scan(&acc.Metadata, &asset, &inputStr, &outputStr); err != nil {
			return nil, s.error(err)
		}

		if asset.Valid {
			assetsVolumes[asset.String] = core.Volumes{
				Input:  core.NewMonetaryInt(0),
				Output: core.NewMonetaryInt(0),
			}

			if inputStr.Valid {
				input, err := core.ParseMonetaryInt(inputStr.String)
				if err != nil {
					return nil, s.error(err)
				}
				assetsVolumes[asset.String] = core.Volumes{
					Input:  input,
					Output: assetsVolumes[asset.String].Output,
				}
			}

			if outputStr.Valid {
				output, err := core.ParseMonetaryInt(outputStr.String)
				if err != nil {
					return nil, s.error(err)
				}
				assetsVolumes[asset.String] = core.Volumes{
					Input:  assetsVolumes[asset.String].Input,
					Output: output,
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, s.error(err)
	}

	res := &core.AccountWithVolumes{
		Account: acc,
		Volumes: assetsVolumes,
	}
	res.Balances = res.Volumes.Balances()

	if s.singleWriter {
		s.cache.Set(address, res.Copy(), cache.NoExpiration)
	}

	return res, nil
}

func (s *Store) CountTransactions(ctx context.Context, q ledger.TransactionsQuery) (uint64, error) {
	var count uint64

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return 0, err
	}

	sb, _ := s.buildTransactionsQuery(Flavor(s.schema.Flavor()), q)
	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	sqlq = fmt.Sprintf(`SELECT count(*) FROM (%s) AS t`, sqlq)
	err = executor.QueryRowContext(ctx, sqlq, args...).Scan(&count)

	return count, s.error(err)
}

func (s *Store) CountAccounts(ctx context.Context, q ledger.AccountsQuery) (uint64, error) {
	executor, err := s.executorProvider(ctx)
	if err != nil {
		return 0, err
	}

	sb, _ := s.buildAccountsQuery(q)
	sqlq, args := sb.Select("count(*)").BuildWithFlavor(s.schema.Flavor())

	var count uint64
	err = executor.QueryRowContext(ctx, sqlq, args...).Scan(&count)
	return count, s.error(err)
}

func (s *Store) GetAssetsVolumes(ctx context.Context, accountAddress string) (core.AssetsVolumes, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("asset", "input", "output")
	sb.From(s.schema.Table("volumes"))
	sb.Where(sb.E("account", accountAddress))

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return nil, err
	}

	q, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := executor.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, s.error(err)
	}
	defer rows.Close()

	volumes := core.AssetsVolumes{}
	for rows.Next() {
		var (
			asset     string
			inputStr  string
			outputStr string
		)
		if err := rows.Scan(&asset, &inputStr, &outputStr); err != nil {
			return nil, s.error(err)
		}

		input, err := core.ParseMonetaryInt(inputStr)
		if err != nil {
			return nil, s.error(err)
		}

		output, err := core.ParseMonetaryInt(outputStr)
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
