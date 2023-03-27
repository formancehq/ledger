package ledger

import (
	"context"
	"math/big"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/uptrace/bun"
)

const (
	volumesTableName = "volumes"
)

type Volumes struct {
	bun.BaseModel `bun:"volumes,alias:volumes"`

	Account string `bun:"account,type:varchar,unique:account_asset"`
	Asset   string `bun:"asset,type:varchar,unique:account_asset"`
	Input   uint64 `bun:"input,type:numeric"`
	Output  uint64 `bun:"output,type:numeric"`
}

func (s *Store) UpdateVolumes(ctx context.Context, volumes core.AccountsAssetsVolumes) error {
	if !s.isInitialized {
		return ErrStoreNotInitialized
	}

	for account, accountVolumes := range volumes {
		for asset, volumes := range accountVolumes {
			v := &Volumes{
				Account: account,
				Asset:   asset,
				Input:   volumes.Input.Uint64(),
				Output:  volumes.Output.Uint64(),
			}

			query := s.schema.NewInsert(volumesTableName).
				Model(v).
				On("CONFLICT (account, asset) DO UPDATE").
				Set("input = EXCLUDED.input, output = EXCLUDED.output").
				String()

			_, err := s.schema.ExecContext(ctx, query)
			if err != nil {
				return s.error(err)
			}
		}
	}

	return nil
}

func (s *Store) GetAssetsVolumes(ctx context.Context, accountAddress string) (core.AssetsVolumes, error) {
	if !s.isInitialized {
		return nil, ErrStoreNotInitialized
	}

	query := s.schema.NewSelect(volumesTableName).
		Model((*Volumes)(nil)).
		Column("asset", "input", "output").
		Where("account = ?", accountAddress).
		String()

	rows, err := s.schema.QueryContext(ctx, query)
	if err != nil {
		return nil, s.error(err)
	}
	defer rows.Close()

	volumes := core.AssetsVolumes{}
	for rows.Next() {
		var (
			asset     string
			inputStr  string
			outputStr string
		)
		if err := rows.Scan(&asset, &inputStr, &outputStr); err != nil {
			return nil, s.error(err)
		}

		input, ok := new(big.Int).SetString(inputStr, 10)
		if !ok {
			panic("unable to restore big int")
		}

		output, ok := new(big.Int).SetString(outputStr, 10)
		if !ok {
			panic("unable to restore big int")
		}

		volumes[asset] = core.Volumes{
			Input:  input,
			Output: output,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, s.error(err)
	}

	return volumes, nil
}
