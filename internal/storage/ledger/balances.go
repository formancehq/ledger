package ledger

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v2/collectionutils"
	"math/big"
	"slices"
	"strings"

	"github.com/formancehq/go-libs/v2/platform/postgres"

	"github.com/formancehq/ledger/internal/tracing"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func (store *Store) lockVolumes(ctx context.Context, accountsWithAssets map[string][]string) error {

	lockKeys := make([]string, 0)
	for account, assets := range accountsWithAssets {
		for _, asset := range assets {
			lockKeys = append(lockKeys, fmt.Sprintf("%s-%s-%s", store.ledger.Name, account, asset))
		}
	}

	// notes(gfyrag): Keep order, it ensures consistent locking order and limit deadlocks
	slices.Sort(lockKeys)

	lockQuery := collectionutils.Map(lockKeys, func(lockKey string) string {
		return fmt.Sprintf(`select pg_advisory_xact_lock(hashtext('%s'));`, lockKey)
	})

	_, err := store.db.NewRaw(strings.Join(lockQuery, "")).Exec(ctx)
	return postgres.ResolveError(err)
}

func (store *Store) GetBalances(ctx context.Context, query ledgercontroller.BalanceQuery) (ledgercontroller.Balances, error) {
	return tracing.TraceWithMetric(
		ctx,
		"GetBalances",
		store.tracer,
		store.getBalancesHistogram,
		func(ctx context.Context) (ledgercontroller.Balances, error) {
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

			if err := store.lockVolumes(ctx, query); err != nil {
				return nil, postgres.ResolveError(err)
			}

			err := store.db.NewSelect().
				ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
				Model(&accountsVolumes).
				Column("accounts_address", "asset").
				ColumnExpr("sum(input) as input").
				ColumnExpr("sum(output) as output").
				Where("("+strings.Join(conditions, ") or (")+")", args...).
				Group("accounts_address", "asset").
				Order("accounts_address", "asset").
				Scan(ctx)
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
