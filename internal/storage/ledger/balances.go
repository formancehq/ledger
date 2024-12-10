package ledger

import (
	"context"
	"math/big"
	"strings"

	"github.com/formancehq/go-libs/v2/platform/postgres"

	"github.com/formancehq/ledger/internal/tracing"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func (s *Store) GetBalances(ctx context.Context, query ledgercontroller.BalanceQuery) (ledgercontroller.Balances, error) {
	return tracing.TraceWithMetric(
		ctx,
		"GetBalances",
		s.tracer,
		s.getBalancesHistogram,
		func(ctx context.Context) (ledgercontroller.Balances, error) {
			conditions := make([]string, 0)
			args := make([]any, 0)
			for account, assets := range query {
				for _, asset := range assets {
					conditions = append(conditions, "ledger = ? and accounts_address = ? and asset = ?")
					args = append(args, s.ledger.Name, account, asset)
				}
			}

			type AccountsVolumesWithLedger struct {
				ledger.AccountsVolumes `bun:",extend"`
				Ledger                 string `bun:"ledger,type:varchar"`
			}

			accountsVolumes := make([]AccountsVolumesWithLedger, 0)
			for account, assets := range query {
				for _, asset := range assets {
					accountsVolumes = append(accountsVolumes, AccountsVolumesWithLedger{
						Ledger: s.ledger.Name,
						AccountsVolumes: ledger.AccountsVolumes{
							Account: account,
							Asset:   asset,
							Input:   new(big.Int),
							Output:  new(big.Int),
						},
					})
				}
			}

			// Try to insert volumes using last move (to keep compat with previous version) or 0 values.
			// This way, if the account has a 0 balance at this point, it will be locked as any other accounts.
			// If the complete sql transaction fails, the account volumes will not be inserted.
			selectMoves := s.db.NewSelect().
				ModelTableExpr(s.GetPrefixedRelationName("moves")).
				DistinctOn("accounts_address, asset").
				Column("accounts_address", "asset").
				ColumnExpr("first_value(post_commit_volumes) over (partition by accounts_address, asset order by seq desc) as post_commit_volumes").
				ColumnExpr("first_value(ledger) over (partition by accounts_address, asset order by seq desc) as ledger").
				Where("("+strings.Join(conditions, ") OR (")+")", args...)

			zeroValuesAndMoves := s.db.NewSelect().
				TableExpr("(?) data", selectMoves).
				Column("ledger", "accounts_address", "asset").
				ColumnExpr("(post_commit_volumes).inputs as input").
				ColumnExpr("(post_commit_volumes).outputs as output").
				UnionAll(
					s.db.NewSelect().
						TableExpr(
							"(?) data",
							s.db.NewSelect().NewValues(&accountsVolumes),
						).
						Column("*"),
				)

			zeroValueOrMoves := s.db.NewSelect().
				TableExpr("(?) data", zeroValuesAndMoves).
				Column("ledger", "accounts_address", "asset", "input", "output").
				DistinctOn("ledger, accounts_address, asset")

			insertDefaultValue := s.db.NewInsert().
				TableExpr(s.GetPrefixedRelationName("accounts_volumes")).
				TableExpr("(" + zeroValueOrMoves.String() + ") data").
				On("conflict (ledger, accounts_address, asset) do nothing").
				Returning("ledger, accounts_address, asset, input, output")

			selectExistingValues := s.db.NewSelect().
				ModelTableExpr(s.GetPrefixedRelationName("accounts_volumes")).
				Column("ledger", "accounts_address", "asset", "input", "output").
				Where("("+strings.Join(conditions, ") OR (")+")", args...).
				For("update").
				// notes(gfyrag): Keep order, it ensures consistent locking order and limit deadlocks
				Order("accounts_address", "asset")

			finalQuery := s.db.NewSelect().
				With("inserted", insertDefaultValue).
				With("existing", selectExistingValues).
				ModelTableExpr(
					"(?) accounts_volumes",
					s.db.NewSelect().
						ModelTableExpr("inserted").
						UnionAll(s.db.NewSelect().ModelTableExpr("existing")),
				).
				Model(&accountsVolumes)

			err := finalQuery.Scan(ctx)
			if err != nil {
				return nil, postgres.ResolveError(err)
			}

			ret := ledgercontroller.Balances{}
			for _, volumes := range accountsVolumes {
				if _, ok := ret[volumes.Account]; !ok {
					ret[volumes.Account] = map[string]*big.Int{}
				}
				ret[volumes.Account][volumes.Asset] = new(big.Int).Sub(volumes.Input, volumes.Output)
			}

			// Fill empty balances with 0 value
			for account, assets := range query {
				if _, ok := ret[account]; !ok {
					ret[account] = map[string]*big.Int{}
				}
				for _, asset := range assets {
					if _, ok := ret[account][asset]; !ok {
						ret[account][asset] = big.NewInt(0)
					}
				}
			}

			return ret, nil
		},
	)
}
