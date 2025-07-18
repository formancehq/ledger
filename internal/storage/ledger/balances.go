package ledger

import (
	"context"
	"math/big"
	"slices"
	"strings"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/tracing"

	ledger "github.com/formancehq/ledger/internal"
)

func (store *Store) GetBalances(ctx context.Context, query BalanceQuery) (ledger.Balances, error) {
	return tracing.TraceWithMetric(
		ctx,
		"GetBalances",
		store.tracer,
		store.getBalancesHistogram,
		func(ctx context.Context) (ledger.Balances, error) {
			conditions := make([]string, 0)
			args := make([]any, 0)
			for account, assets := range query {
				for _, asset := range assets {
					conditions = append(conditions, "ledger = ? and accounts_address = ? and asset = ?")
					args = append(args, store.ledger.Name, account, asset)
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
						Ledger: store.ledger.Name,
						AccountsVolumes: ledger.AccountsVolumes{
							Account: account,
							Asset:   asset,
							Input:   new(big.Int),
							Output:  new(big.Int),
						},
					})
				}
			}

			// prevent deadlocks by sorting the accountsVolumes slice
			slices.SortStableFunc(accountsVolumes, func(i, j AccountsVolumesWithLedger) int {
				if i.Account < j.Account {
					return -1
				} else if i.Account > j.Account {
					return 1
				} else if i.Asset < j.Asset {
					return -1
				} else if i.Asset > j.Asset {
					return 1
				} else {
					return 0
				}
			})

			err := store.db.NewSelect().
				With(
					"ins",
					// Try to insert volumes with 0 values.
					// This way, if the account has a 0 balance at this point, it will be locked as any other accounts.
					// It the complete sql transaction fail, the account volumes will not be inserted.
					store.db.NewInsert().
						Model(&accountsVolumes).
						ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
						On("conflict do nothing"),
				).
				Model(&accountsVolumes).
				ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
				Column("accounts_address", "asset", "input", "output").
				Where("("+strings.Join(conditions, ") OR (")+")", args...).
				For("update").
				// notes(gfyrag): Keep order, it ensures consistent locking order and limit deadlocks
				Order("accounts_address", "asset").
				Scan(ctx)
			if err != nil {
				return nil, postgres.ResolveError(err)
			}

			ret := ledger.Balances{}
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
