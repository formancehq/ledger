package sqlstorage

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
)

func (s *Store) updateVolumes(ctx context.Context, volumes core.AccountsAssetsVolumes) error {
	for account, accountVolumes := range volumes {
		accountBy, err := json.Marshal(strings.Split(account, ":"))
		if err != nil {
			panic(err)
		}

		for asset, volumes := range accountVolumes {
			ib := sqlbuilder.NewInsertBuilder()
			inputArg := ib.Var(volumes.Input.String())
			outputArg := ib.Var(volumes.Output.String())
			ib = ib.InsertInto(s.schema.Table("volumes"))

			switch s.schema.Flavor() {
			case sqlbuilder.PostgreSQL:
				ib = ib.Cols("account", "asset", "input", "output", "account_json").
					Values(account, asset, volumes.Input.String(), volumes.Output.String(), accountBy).
					SQL("ON CONFLICT (account, asset) DO UPDATE SET input = " + inputArg + ", output = " + outputArg)
			case sqlbuilder.SQLite:
				ib = ib.Cols("account", "asset", "input", "output").
					Values(account, asset, volumes.Input.String(), volumes.Output.String()).
					SQL("ON CONFLICT (account, asset) DO UPDATE SET input = " + inputArg + ", output = " + outputArg)
			}

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
