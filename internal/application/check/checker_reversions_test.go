package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// writeReversionWord persists one reversion bitset word the way the FSM does.
func writeReversionWord(t *testing.T, store *dal.Store, ledger string, wordIndex, value uint64) {
	t.Helper()

	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveReversionWord(batch, ledger, wordIndex, value))
	require.NoError(t, batch.Commit())
}

// collectReversionEvents runs compareReversions against the store's persisted
// bitsets with the given audit-derived set and returns the REVERTED_MISMATCH
// errors.
func collectReversionEvents(t *testing.T, store *dal.Store, derived map[string]*bitset.Bitset, knownLedgers map[string]struct{}) []*servicepb.CheckStoreError {
	t.Helper()

	checker := NewChecker(store, attributes.New(), "reversions-cluster", nil, nil, logging.Testing())

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	var got []*servicepb.CheckStoreError

	require.NoError(t, checker.compareReversions(handle, derived, knownLedgers, func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok &&
			e.Error.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERTED_MISMATCH {
			got = append(got, e.Error)
		}
	}))

	return got
}

func reversionSet(ids ...uint64) map[string]*bitset.Bitset {
	bs := &bitset.Bitset{}
	for _, id := range ids {
		bs.Set(id)
	}

	return map[string]*bitset.Bitset{"ledger-a": bs}
}

func TestCompareReversions_Match(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeReversionWord(t, store, "ledger-a", 0, 1<<3|1<<7)
	writeReversionWord(t, store, "ledger-a", 1, 1<<6) // tx 70

	got := collectReversionEvents(t, store, reversionSet(3, 7, 70),
		map[string]struct{}{"ledger-a": {}})
	require.Empty(t, got)
}

func TestCompareReversions_MissingBitFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)

	got := collectReversionEvents(t, store, reversionSet(3),
		map[string]struct{}{"ledger-a": {}})
	require.Len(t, got, 1)
	require.Equal(t, "ledger-a", got[0].GetLedger())
	require.Equal(t, uint64(3), got[0].GetTransactionId())
	require.Contains(t, got[0].GetMessage(), "reversion bit missing")
}

func TestCompareReversions_UnauditedBitFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeReversionWord(t, store, "ledger-a", 1, 1<<6) // tx 70, no audit backing

	got := collectReversionEvents(t, store, nil,
		map[string]struct{}{"ledger-a": {}})
	require.Len(t, got, 1)
	require.Equal(t, uint64(70), got[0].GetTransactionId())
	require.Contains(t, got[0].GetMessage(), "unaudited reversion bit")
}

// A persisted pending-cleanup marker is itself an unverified projection: it
// must not exempt an audit-live ledger from the comparison, or a forged
// marker hides bitset tampering.
func TestCompareReversions_PendingCleanupMarkerDoesNotHideMismatch(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeReversionWord(t, store, "ledger-a", 0, 1<<5)

	batch := store.OpenWriteSession()
	require.NoError(t, state.SavePendingLedgerCleanup(batch, "ledger-a", 9))
	require.NoError(t, batch.Commit())

	got := collectReversionEvents(t, store, nil,
		map[string]struct{}{"ledger-a": {}})
	require.Len(t, got, 1)
	require.Equal(t, uint64(5), got[0].GetTransactionId())
	require.Contains(t, got[0].GetMessage(), "unaudited reversion bit")
}

// DeleteLedger removes the reversion rows at apply on both the live path and
// the replay, so stored rows for a ledger the audit does not know as live are
// never legitimate.
func TestCompareReversions_NonLiveLedgerRowsFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeReversionWord(t, store, "ghost", 0, 1<<2)

	got := collectReversionEvents(t, store, nil, map[string]struct{}{"ledger-a": {}})
	require.Len(t, got, 1)
	require.Equal(t, "ghost", got[0].GetLedger())
	require.Contains(t, got[0].GetMessage(), "non-live ledger")
}

// Rows that fail to decode must surface as events, not silently narrow the
// comparison.
func TestCompareReversions_MalformedRowFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte{0x03, 0x01, 'x'}, []byte{0x01}))
	require.NoError(t, batch.Commit())

	got := collectReversionEvents(t, store, nil, map[string]struct{}{})
	require.Len(t, got, 1)
	require.Contains(t, got[0].GetMessage(), "malformed reversion row")
}
