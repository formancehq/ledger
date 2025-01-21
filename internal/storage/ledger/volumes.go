package ledger

import (
	"context"
	"github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/tracing"
)

func (store *Store) UpdateVolumes(ctx context.Context, accountVolumes ...ledger.AccountsVolumes) error {
	return tracing.SkipResult(tracing.TraceWithMetric(
		ctx,
		"UpdateBalances",
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
