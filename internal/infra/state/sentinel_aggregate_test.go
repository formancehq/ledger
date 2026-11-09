package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func seedSentinelVolume(t *testing.T, store *dal.Store, attr *attributes.Attribute[*raftcmdpb.VolumePair], account string, input, output *uint256.Int) {
	t.Helper()
	key := domain.NewVolumeKey("test", account, "USD", "")
	batch := store.OpenWriteSession()
	_, err := attr.Set(batch, key.Bytes(), &raftcmdpb.VolumePair{Input: commonpb.NewUint256(input), Output: commonpb.NewUint256(output)})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

// Each account volume fits in uint256, so the FSM accepts them, but their
// ledger-wide sum does not. The sentinel sums exactly, so the balanced ledger
// passes.
func TestVerifyAggregatedVolumesBalancedSumsPastUint256(t *testing.T) {
	t.Parallel()
	_, machine, store := newTestBuffer(t)
	attr := machine.Registry.Attrs.Volume
	maxVal := new(uint256.Int).SetAllOne()
	seedSentinelVolume(t, store, attr, "a", uint256.NewInt(0), maxVal)
	seedSentinelVolume(t, store, attr, "b", uint256.NewInt(0), uint256.NewInt(1))
	seedSentinelVolume(t, store, attr, "c", maxVal, uint256.NewInt(0))
	seedSentinelVolume(t, store, attr, "d", uint256.NewInt(1), uint256.NewInt(0))

	reader, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	require.NoError(t, verifyAggregatedVolumesBalanced(reader, attr, []string{"test"}, nil, 42, machine.logger))
}

func TestVerifyAggregatedVolumesBalancedRejectsImbalance(t *testing.T) {
	t.Parallel()
	_, machine, store := newTestBuffer(t)
	attr := machine.Registry.Attrs.Volume
	seedSentinelVolume(t, store, attr, "a", uint256.NewInt(0), uint256.NewInt(10))
	seedSentinelVolume(t, store, attr, "b", uint256.NewInt(9), uint256.NewInt(0))

	reader, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	err = verifyAggregatedVolumesBalanced(reader, attr, []string{"test"}, nil, 42, machine.logger)
	require.ErrorContains(t, err, "aggregated volume imbalance")
}

func TestVerifyAggregatedVolumesBalancedRejectsDeletedLedgerRows(t *testing.T) {
	t.Parallel()
	_, machine, store := newTestBuffer(t)
	attr := machine.Registry.Attrs.Volume
	seedSentinelVolume(t, store, attr, "a", uint256.NewInt(0), uint256.NewInt(10))
	seedSentinelVolume(t, store, attr, "b", uint256.NewInt(10), uint256.NewInt(0))

	reader, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	err = verifyAggregatedVolumesBalanced(reader, attr, []string{"test"}, map[string]struct{}{"test": {}}, 42, machine.logger)
	require.ErrorContains(t, err, "still has volumes after commit")
}
