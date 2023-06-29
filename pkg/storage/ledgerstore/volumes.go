package ledgerstore

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger/pkg/core"
	storageerrors "github.com/formancehq/ledger/pkg/storage"
)

func (s *Store) GetAssetsVolumes(ctx context.Context, accountAddress string) (core.VolumesByAssets, error) {
	moves := make([]Move, 0)

	err := s.schema.NewSelect(MovesTableName).
		Model(&moves).
		Where("account = ?", accountAddress).
		ColumnExpr("asset").
		ColumnExpr(fmt.Sprintf(`"%s".first(post_commit_input_value order by timestamp desc) as post_commit_input_value`, s.schema.Name())).
		ColumnExpr(fmt.Sprintf(`"%s".first(post_commit_output_value order by timestamp desc) as post_commit_output_value`, s.schema.Name())).
		GroupExpr("account, asset").
		Scan(ctx)
	if err != nil {
		return nil, storageerrors.PostgresError(err)
	}

	volumes := core.VolumesByAssets{}
	for _, move := range moves {
		volumes[move.Asset] = core.NewEmptyVolumes().
			WithInput((*big.Int)(move.PostCommitInputVolume)).
			WithOutput((*big.Int)(move.PostCommitOutputVolume))
	}

	return volumes, nil
}
