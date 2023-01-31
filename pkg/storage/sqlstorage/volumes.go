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
			inputArg := ib.Var(volumes.Input.String())
			outputArg := ib.Var(volumes.Output.String())
			ib.
				InsertInto(s.schema.Table("volumes")).
				Cols("account", "asset", "input", "output").
				Values(account, asset, volumes.Input.String(), volumes.Output.String()).
				SQL("ON CONFLICT (account, asset) DO UPDATE SET input = " + inputArg + ", output = " + outputArg)

			sqlq, args := ib.BuildWithFlavor(s.schema.Flavor())

			executor, err := s.executorProvider(ctx)
			if err != nil {
				return err
			}

			_, err = executor.ExecContext(ctx, sqlq, args...)
			if err != nil {
				return s.error(err)
			}
		}
	}

	return nil
}
