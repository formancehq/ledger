package sqlstorage

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
)

func (s *Store) updateVolumes(ctx context.Context, volumes core.AccountsAssetsVolumes) error {

	if s.singleInstance {
		storage.OnTransactionCommitted(ctx, func() {
			for address, accountVolumes := range volumes {
				entry, ok := s.cache.Get(address)
				if ok {
					account := entry.(*core.AccountWithVolumes)
					for asset, volumes := range accountVolumes {
						account.Volumes[asset] = volumes
						account.Balances[asset] = volumes.Balance()
					}
				}
			}
		})
	}

	for address, accountVolumes := range volumes {

		accountBy, err := json.Marshal(strings.Split(address, ":"))
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
					Values(address, asset, volumes.Input.String(), volumes.Output.String(), accountBy).
					SQL("ON CONFLICT (account, asset) DO UPDATE SET input = " + inputArg + ", output = " + outputArg)
			case sqlbuilder.SQLite:
				ib = ib.Cols("account", "asset", "input", "output").
					Values(address, asset, volumes.Input.String(), volumes.Output.String()).
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
