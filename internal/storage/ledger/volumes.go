package ledger

import (
	"context"
	"github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/tracing"
)

func (s *Store) UpdateVolumes(ctx context.Context, accountVolumes ...ledger.AccountsVolumes) (ledger.PostCommitVolumes, error) {
	return tracing.TraceWithMetric(
		ctx,
		"UpdateBalances",
		s.tracer,
		s.updateBalancesHistogram,
		func(ctx context.Context) (ledger.PostCommitVolumes, error) {

			type AccountsVolumesWithLedger struct {
				ledger.AccountsVolumes `bun:",extend"`
				Ledger                 string `bun:"ledger,type:varchar"`
			}

			accountsVolumesWithLedger := collectionutils.Map(accountVolumes, func(from ledger.AccountsVolumes) AccountsVolumesWithLedger {
				return AccountsVolumesWithLedger{
					AccountsVolumes: from,
					Ledger:          s.ledger.Name,
				}
			})

			_, err := s.db.NewInsert().
				Model(&accountsVolumesWithLedger).
				ModelTableExpr(s.GetPrefixedRelationName("accounts_volumes")).
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