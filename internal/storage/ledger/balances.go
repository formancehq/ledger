package ledger

import (
	"context"
	"math/big"
	"slices"
	"strings"

	"github.com/formancehq/go-libs/v2/platform/postgres"

	"github.com/formancehq/ledger/internal/tracing"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

const fillAccountsVolumeHistoryMigration = 21

func (store *Store) GetBalances(ctx context.Context, query ledgercontroller.BalanceQuery) (ledgercontroller.Balances, error) {
	lastVersion, err := store.bucket.GetLastVersion(ctx, store.db)
	if err != nil {
		return nil, err
	}

	if lastVersion >= fillAccountsVolumeHistoryMigration {
		return store.GetBalancesAfterUpgrade(ctx, query)
	} else {
		return store.GetBalancesWhenUpgrading(ctx, query)
	}
}

func (store *Store) GetBalancesWhenUpgrading(ctx context.Context, query ledgercontroller.BalanceQuery) (ledgercontroller.Balances, error) {
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
				Ledger                 string `bun:"ledger,type:varchar"`
				ledger.AccountsVolumes `bun:",extend"`
				Priority               int `bun:"priority"` // for ordering (keep at 0)
			}

			defaultAccountsVolumes := make([]AccountsVolumesWithLedger, 0)
			for account, assets := range query {
				for _, asset := range assets {
					defaultAccountsVolumes = append(defaultAccountsVolumes, AccountsVolumesWithLedger{
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
			slices.SortStableFunc(defaultAccountsVolumes, func(i, j AccountsVolumesWithLedger) int {
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

			// Try to insert volumes using last move (to keep compat with previous version) or 0 values.
			// This way, if the account has a 0 balance at this point, it will be locked as any other accounts.
			// If the complete sql transaction fails, the account volumes will not be inserted.
			selectMoves := store.db.NewSelect().
				ModelTableExpr(store.GetPrefixedRelationName("moves")).
				DistinctOn("accounts_address, asset").
				Column("accounts_address", "asset").
				ColumnExpr("first_value(post_commit_volumes) over (partition by accounts_address, asset order by seq desc) as post_commit_volumes").
				ColumnExpr("first_value(ledger) over (partition by accounts_address, asset order by seq desc) as ledger").
				Where("("+strings.Join(conditions, ") OR (")+")", args...)

			zeroValuesAndMoves := store.db.NewSelect().
				TableExpr("(?) data", selectMoves).
				Column("ledger", "accounts_address", "asset").
				ColumnExpr("(post_commit_volumes).inputs as input").
				ColumnExpr("(post_commit_volumes).outputs as output").
				ColumnExpr("1 as priority").
				UnionAll(
					store.db.NewSelect().
						TableExpr(
							"(?) data",
							store.db.NewSelect().
								NewValues(&defaultAccountsVolumes),
						).
						Column("*"),
				)

			zeroValueOrMoves := store.db.NewSelect().
				TableExpr("(?) data", zeroValuesAndMoves).
				Column("ledger", "accounts_address", "asset").
				ColumnExpr("first_value(input) over (partition by ledger, accounts_address, asset order by priority desc) as input").
				ColumnExpr("first_value(output) over (partition by ledger, accounts_address, asset order by priority desc) as output").
				DistinctOn("ledger, accounts_address, asset")

			insertDefaultValue := store.db.NewInsert().
				TableExpr(store.GetPrefixedRelationName("accounts_volumes")).
				TableExpr("(" + zeroValueOrMoves.String() + ") data").
				On("conflict (ledger, accounts_address, asset) do nothing").
				Returning("ledger, accounts_address, asset, input, output")

			selectExistingValues := store.db.NewSelect().
				ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
				Column("ledger", "accounts_address", "asset", "input", "output").
				Where("("+strings.Join(conditions, ") OR (")+")", args...).
				For("update").
				// notes(gfyrag): Keep order, it ensures consistent locking order and limit deadlocks
				Order("accounts_address", "asset")

			accountsVolumes := make([]ledger.AccountsVolumes, 0)
			finalQuery := store.db.NewSelect().
				With("inserted", insertDefaultValue).
				With("existing", selectExistingValues).
				ModelTableExpr(
					"(?) accounts_volumes",
					store.db.NewSelect().
						ModelTableExpr("inserted").
						UnionAll(store.db.NewSelect().ModelTableExpr("existing")),
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

func (store *Store) GetBalancesAfterUpgrade(ctx context.Context, query ledgercontroller.BalanceQuery) (ledgercontroller.Balances, error) {
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
