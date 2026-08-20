package processing

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestPostCommitVolumeAccumulatorInlineAndPromotion(t *testing.T) {
	t.Parallel()

	var accumulator postCommitVolumeAccumulator
	accumulator.init(6)

	keys := make([]domain.VolumeKey, 10)
	for i := range keys {
		keys[i] = domain.NewVolumeKey("ledger", fmt.Sprintf("account:%02d", 9-i), "USD", "")
		accumulator.capture(keys[i], testVolumePair(uint64(i+1), uint64((i+1)*10)))

		if i == 3 {
			// Replacement while the accumulator is still using its linear inline
			// lookup must keep one row and expose the latest value.
			accumulator.capture(keys[0], testVolumePair(101, 102))
		}
	}

	// Replacement after promotion to the index map exercises the other lookup
	// path and must retain exactly the same semantics.
	accumulator.capture(keys[5], testVolumePair(201, 202))

	got := accumulator.build()
	require.Len(t, got.GetVolumes(), len(keys))

	// build sorts by (account, asset, color), independently of capture order.
	for i, row := range got.GetVolumes() {
		require.Equal(t, fmt.Sprintf("account:%02d", i), row.GetAccount())
	}

	first := findFlatPostCommitVolume(got, keys[0])
	require.NotNil(t, first)
	require.Equal(t, "101", first.GetInput())
	require.Equal(t, "102", first.GetOutput())

	afterPromotion := findFlatPostCommitVolume(got, keys[5])
	require.NotNil(t, afterPromotion)
	require.Equal(t, "201", afterPromotion.GetInput())
	require.Equal(t, "202", afterPromotion.GetOutput())
}

func TestPostCommitVolumeAccumulatorKeepsColorsDistinct(t *testing.T) {
	t.Parallel()

	var accumulator postCommitVolumeAccumulator
	accumulator.init(1)
	accumulator.capture(domain.NewVolumeKey("ledger", "account", "USD", ""), testVolumePair(1, 2))
	accumulator.capture(domain.NewVolumeKey("ledger", "account", "USD", "BLUE"), testVolumePair(3, 4))

	got := accumulator.build()
	require.Len(t, got.GetVolumes(), 2)
	require.Equal(t, "", got.GetVolumes()[0].GetColor())
	require.Equal(t, "BLUE", got.GetVolumes()[1].GetColor())
}

func testVolumePair(input, output uint64) *raftcmdpb.VolumePair {
	return &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(input),
		Output: commonpb.NewUint256FromUint64(output),
	}
}

func findFlatPostCommitVolume(pcv *commonpb.PostCommitVolumes, key domain.VolumeKey) *commonpb.PostCommitVolume {
	for _, row := range pcv.GetVolumes() {
		if row.GetAccount() == key.Account && row.GetAsset() == key.Asset && row.GetColor() == key.Color {
			return row
		}
	}

	return nil
}
