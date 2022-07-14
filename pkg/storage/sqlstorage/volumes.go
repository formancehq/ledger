package sqlstorage

import (
	"context"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
)

/**

INSERT INTO "VAR_LEDGER_NAME".volumes (account, asset, input, output)
VALUES (p.source, p.asset, 0, p.amount::bigint)
ON CONFLICT (account, asset) DO UPDATE SET output = p.amount::bigint + (
	SELECT output
	FROM "VAR_LEDGER_NAME".volumes
	WHERE account = p.source
	  AND asset = p.asset
);
*/

func (s *Store) updateVolumes(ctx context.Context, exec executor, volumes core.AccountsAssetsVolumes) error {

	for account, accountVolumes := range volumes {
		for asset, volumes := range accountVolumes {
			//ub := sqlbuilder.NewUpdateBuilder()
			//sqlq, args := ub.
			//	Update(s.schema.Table("volumes")).
			//	Set(ub.Assign("input", volumes.Input), ub.Assign("output", volumes.Output)).
			//	Where(ub.And(ub.E("account", account), ub.E("asset", asset))).
			//	BuildWithFlavor(s.schema.Flavor())

			ib := sqlbuilder.NewInsertBuilder()
			inputArg := ib.Var(volumes.Input)
			outputArg := ib.Var(volumes.Output)
			ib.
				InsertInto(s.schema.Table("volumes")).
				Cols("account", "asset", "input", "output").
				Values(account, asset, volumes.Input, volumes.Output).
				SQL("ON CONFLICT (account, asset) DO UPDATE SET input = " + inputArg + ", output = " + outputArg)

			sqlq, args := ib.BuildWithFlavor(s.schema.Flavor())

			_, err := exec.ExecContext(ctx, sqlq, args...)
			if err != nil {
				return s.error(err)
			}
		}
	}

	return nil
}
