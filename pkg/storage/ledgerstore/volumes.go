package ledgerstore

import (
	"context"
	"math/big"

	"github.com/formancehq/ledger/pkg/core"
	storageerrors "github.com/formancehq/ledger/pkg/storage/errors"
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

func (s *Store) UpdateVolumes(ctx context.Context, volumes ...core.AccountsAssetsVolumes) error {
	if !s.isInitialized {
		return storageerrors.StorageError(storageerrors.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "update_volumes")
	defer recordMetrics()

	volumesMap := make(map[string]*Volumes)
	for _, vs := range volumes {
		for account, accountVolumes := range vs {
			for asset, volumes := range accountVolumes {
				// De-duplicate same volumes to only have the last version
				volumesMap[account+asset] = &Volumes{
					Account: account,
					Asset:   asset,
					Input:   volumes.Input.Uint64(),
					Output:  volumes.Output.Uint64(),
				}
			}
		}
	}

	vls := make([]*Volumes, 0, len(volumes))
	for _, v := range volumesMap {
		vls = append(vls, v)
	}

	query := s.schema.NewInsert(volumesTableName).
		Model(&vls).
		On("CONFLICT (account, asset) DO UPDATE").
		Set("input = EXCLUDED.input, output = EXCLUDED.output").
		String()

	_, err := s.schema.ExecContext(ctx, query)
	return storageerrors.PostgresError(err)
}

func (s *Store) GetAssetsVolumes(ctx context.Context, accountAddress string) (core.AssetsVolumes, error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storageerrors.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "get_assets_volumes")
	defer recordMetrics()

	query := s.schema.NewSelect(volumesTableName).
		Model((*Volumes)(nil)).
		Column("asset", "input", "output").
		Where("account = ?", accountAddress).
		String()

	rows, err := s.schema.QueryContext(ctx, query)
	if err != nil {
		return nil, storageerrors.PostgresError(err)
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
			return nil, storageerrors.PostgresError(err)
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
		return nil, storageerrors.PostgresError(err)
	}

	return volumes, nil
}
