package sqlstorage

import (
	"context"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
)

func (s *Store) updateVolumes(ctx context.Context, volumes core.AccountsAssetsVolumes) error {

	for account, accountVolumes := range volumes {
		for asset, volumes := range accountVolumes {
			ib := sqlbuilder.NewInsertBuilder()
			inputArg := ib.Var(volumes.Input)
			outputArg := ib.Var(volumes.Output)
			ib.
				InsertInto(s.schema.Table("volumes")).
				Cols("account", "asset", "input", "output").
				Values(account, asset, volumes.Input, volumes.Output).
				SQL("ON CONFLICT (account, asset) DO UPDATE SET input = " + inputArg + ", output = " + outputArg)

			sqlq, args := ib.BuildWithFlavor(s.schema.Flavor())

			_, err := s.getExecutorFromContext(ctx).ExecContext(ctx, sqlq, args...)
			if err != nil {
				return s.error(err)
			}
		}
	}

	return nil
}
