package ledger

import (
	"context"
	"github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/tracing"
)

func (store *Store) UpdateVolumes(ctx context.Context, accountVolumes ...ledger.AccountsVolumes) error {
	upToDate, err := store.bucket.IsUpToDate(ctx)
	if err != nil {
		return err
	}
	if !upToDate {
		_, err := store.updateVolumesLegacy(ctx, accountVolumes...)
		return err
	}

	return tracing.SkipResult(tracing.TraceWithMetric(
		ctx,
		"UpdateVolumes",
		store.tracer,
		store.updateBalancesHistogram,
		tracing.NoResult(func(ctx context.Context) error {

			type AccountsVolumesWithLedger struct {
				ledger.AccountsVolumes `bun:",extend"`
				Ledger                 string `bun:"ledger,type:varchar"`
			}

			accountsVolumesWithLedger := collectionutils.Map(accountVolumes, func(from ledger.AccountsVolumes) AccountsVolumesWithLedger {
				return AccountsVolumesWithLedger{
					AccountsVolumes: from,
					Ledger:          store.ledger.Name,
				}
			})

			_, err := store.db.NewInsert().
				Model(&accountsVolumesWithLedger).
				ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
				Exec(ctx)
			if err != nil {
				return postgres.ResolveError(err)
			}

			return err
		},
		)))
}

func (store *Store) updateVolumesLegacy(ctx context.Context, accountVolumes ...ledger.AccountsVolumes) (ledger.PostCommitVolumes, error) {
	return tracing.TraceWithMetric(
		ctx,
		"UpdateVolumes_Legacy",
		store.tracer,
		store.updateBalancesHistogram,
		func(ctx context.Context) (ledger.PostCommitVolumes, error) {

			type AccountsVolumesWithLedger struct {
				ledger.AccountsVolumes `bun:",extend"`
				Ledger                 string `bun:"ledger,type:varchar"`
			}

			accountsVolumesWithLedger := collectionutils.Map(accountVolumes, func(from ledger.AccountsVolumes) AccountsVolumesWithLedger {
				return AccountsVolumesWithLedger{
					AccountsVolumes: from,
					Ledger:          store.ledger.Name,
				}
			})

			_, err := store.db.NewInsert().
				Model(&accountsVolumesWithLedger).
				ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
				On("conflict (ledger, accounts_address, asset) do update").
				Set("input = accounts_volumes.input + excluded.input").
				Set("output = accounts_volumes.output + excluded.output").
				Returning("input, output").
				Exec(ctx)
			if err != nil {
				return nil, postgres.ResolveError(err)
			}

			ret := ledger.PostCommitVolumes{}
			for _, volumes := range accountVolumes {
				if _, ok := ret[volumes.Account]; !ok {
					ret[volumes.Account] = map[string]ledger.Volumes{}
				}
				ret[volumes.Account][volumes.Asset] = ledger.Volumes{
					Input:  volumes.Input,
					Output: volumes.Output,
				}
			}

			return ret, err
		},
	)
}
