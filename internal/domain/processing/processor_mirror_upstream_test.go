package processing

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	v2 "github.com/formancehq/ledger/v3/internal/adapter/v2"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// The fixtures are serialized by the actual v2.4.7 log payload types (see
// adapter/v2/testdata/README.md), independently of the mirror decoder. This
// composes translation with application; it does not exercise PostgreSQL I/O.
func TestMirrorIngest_UpstreamRevertPreservesOriginalIdentity(t *testing.T) {
	t.Parallel()

	const ledger = "mirror-ledger"
	logs := make([]v2.V2Log, 0, 3)
	for i, fixture := range []struct {
		name string
		kind string
		date string
	}{
		{"v2.4.7-new-transaction-0.json", "NEW_TRANSACTION", "2024-01-01T00:00:00Z"},
		{"v2.4.7-new-transaction-1.json", "NEW_TRANSACTION", "2024-01-01T00:00:00Z"},
		{"v2.4.7-reverted-transaction-1.json", "REVERTED_TRANSACTION", "2024-01-01T01:00:00Z"},
	} {
		data, err := os.ReadFile("../../adapter/v2/testdata/" + fixture.name)
		require.NoError(t, err)
		// Positive, contiguous source IDs isolate the payload regression from
		// source pagination and the separate source-log-zero policy.
		logs = append(logs, v2.V2Log{ID: uint64(i + 1), Type: fixture.kind, Date: fixture.date, Data: data})
	}
	orders, nextLogID, nextTxID, err := v2.TranslateBatch(ledger, logs, 1, 0, nil)
	require.NoError(t, err)
	require.Len(t, orders, 3)
	require.Equal(t, uint64(4), nextLogID)
	require.Equal(t, uint64(3), nextTxID)

	store := NewMockScope(gomock.NewController(t))
	expectDefaultMetadataLimits(store)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)
	info := &commonpb.LedgerInfo{Name: ledger, Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR}
	expectGetLedger(store, domain.LedgerKey{Name: ledger}, info.AsReader(), nil).AnyTimes()
	store.EXPECT().GetDate().Return((&commonpb.Timestamp{Data: 1}).AsReader()).AnyTimes()
	store.EXPECT().GetNextSequenceID().Return(uint64(1)).Times(3)

	boundaries := &raftcmdpb.LedgerBoundaries{NextLogId: 1}
	boundaryStub := setupBoundariesStub(store)
	boundaryStub.onGet(func(key domain.LedgerKey) (raftcmdpb.LedgerBoundariesReader, error) {
		require.Equal(t, domain.LedgerKey{Name: ledger}, key)

		return boundaries.AsReader(), nil
	})
	boundaryStub.onPut(func(_ domain.LedgerKey, value *raftcmdpb.LedgerBoundaries) { boundaries = value.CloneVT() })

	states := make(map[domain.TransactionKey]*commonpb.TransactionState)
	stateStub, _ := stubsFor(store).transactionStatesStubFor(store)
	stateStub.onPut(func(key domain.TransactionKey, value *commonpb.TransactionState) {
		states[key] = value.CloneVT()
		stateStub.expectGet(key, states[key].AsReader(), nil)
	})
	volumes := make(map[domain.VolumeKey]*raftcmdpb.VolumePair)
	volumeStub := setupVolumesStub(store)
	volumeStub.onPut(func(key domain.VolumeKey, value *raftcmdpb.VolumePair) {
		volumes[key] = value.CloneVT()
		volumeStub.expectGet(key, volumes[key].AsReader(), nil)
	})
	reverted := make(map[domain.TransactionKey]bool)
	store.EXPECT().PutReverted(gomock.Any(), true).Do(func(key domain.TransactionKey, value bool) {
		reverted[key] = value
	}).Times(1)

	for i := range 2 {
		result, err := processor.ProcessOrder(orders[i], store)
		require.NoError(t, err)
		require.Equal(t, uint64(i), result.GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetId())
		require.Equal(t, uint64(i+1), boundaries.GetLastMirrorV2LogId())
	}
	independentKey := domain.TransactionKey{LedgerName: ledger, ID: 0}
	originalKey := domain.TransactionKey{LedgerName: ledger, ID: 1}
	compensatingKey := domain.TransactionKey{LedgerName: ledger, ID: 2}
	independentBefore := states[independentKey].CloneVT()
	result, err := processor.ProcessOrder(orders[2], store)
	require.NoError(t, err)
	compensating := result.GetApply().GetLog().GetData().GetRevertedTransaction()
	require.NotNil(t, compensating)

	// Balances and source progress can look correct even while the original
	// identity is corrupted. Assert them before checking the two links.
	for _, want := range []struct {
		account string
		input   uint64
		output  uint64
	}{
		{"world", 100, 125},
		{"alice", 100, 100},
		{"unrelated", 25, 0},
	} {
		volume := volumes[domain.NewVolumeKey(ledger, want.account, "USD", "")]
		require.NotNil(t, volume)
		require.Equal(t, want.input, volume.GetInput().ToBigInt().Uint64(), want.account)
		require.Equal(t, want.output, volume.GetOutput().ToBigInt().Uint64(), want.account)
	}
	require.Equal(t, uint64(3), boundaries.GetLastMirrorV2LogId())
	require.Equal(t, uint64(4), boundaries.GetNextLogId())
	require.Equal(t, uint64(3), boundaries.GetNextTransactionId())
	require.Equal(t, uint64(2), compensating.GetRevertTransaction().GetId())
	require.Contains(t, states, compensatingKey)

	require.Equal(t, map[domain.TransactionKey]bool{originalKey: true}, reverted,
		"nested upstream revertedTransaction.id must mark tx1, never independent tx0")
	require.Equal(t, independentBefore, states[independentKey], "independent tx0 must remain unchanged")
	require.Equal(t, uint64(2), states[originalKey].GetRevertedByTransaction())
	require.Equal(t, uint64(time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC).UnixMicro()), states[originalKey].GetRevertedAt().GetData())
	require.Equal(t, uint64(1), states[compensatingKey].GetRevertsTransaction())
	require.Equal(t, uint64(1), compensating.GetRevertedTransactionId())
	require.Equal(t, uint64(1), compensating.GetRevertTransaction().GetRevertsTransaction())
}
