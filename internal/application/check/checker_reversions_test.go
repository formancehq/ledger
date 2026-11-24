package check

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// writeReversionWord persists one reversion bitset word the way the FSM does.
func writeReversionWord(t *testing.T, store *dal.Store, ledger string, wordIndex, value uint64) {
	t.Helper()

	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveReversionWord(batch, ledger, wordIndex, value))
	require.NoError(t, batch.Commit())
}

// The audit and logs remain intact while the only persisted reversion word is
// moved to a key that decodes to the same ledger and word index.
func TestCheckerRejectsNoncanonicalReversionWord(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	engine.processAndCommit(createLedgerOrder("ledger-a"))
	engine.processAndCommit(createTransactionOrder("ledger-a", true, &commonpb.Posting{
		Source: "world", Destination: "alice", Asset: "USD", Amount: commonpb.NewUint256FromUint64(1),
	}))
	engine.processAndCommit(engine.revertTransactionOrder("ledger-a", 1))
	require.Empty(t, collectCheckErrors(t, engine.store, engine.attrs))

	canonical := reversionWordKey("ledger-a", 0)
	alias := bytes.Clone(canonical)
	alias[2+len("ledger-a")+1] = 'x'

	handle, err := engine.store.NewReadHandle()
	require.NoError(t, err)
	word, closer, err := handle.Get(canonical)
	require.NoError(t, err)
	require.NotNil(t, closer)
	word = bytes.Clone(word)
	require.NoError(t, closer.Close())
	require.NoError(t, handle.Close())

	batch := engine.store.OpenWriteSession()
	require.NoError(t, batch.DeleteKey(canonical))
	require.NoError(t, batch.SetBytes(alias, word))
	require.NoError(t, batch.Commit())

	got := collectCheckErrors(t, engine.store, engine.attrs)
	var aliasFound, missingFound bool
	for _, event := range got {
		if event.GetErrorType() != servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERTED_MISMATCH {
			continue
		}
		if strings.Contains(event.GetMessage(), "malformed reversion row") &&
			strings.Contains(event.GetMessage(), "noncanonical key") {
			aliasFound = true
		}
		if strings.Contains(event.GetMessage(), "reversion bit missing for tx 1") {
			missingFound = true
		}
	}
	require.True(t, aliasFound, "the aliased physical row must be reported")
	require.True(t, missingFound, "the alias must not satisfy the audit-derived word")
}

func reversionWordKey(ledger string, index uint64) []byte {
	key := make([]byte, 2+dal.LedgerNameFixedSize+8)
	key[0], key[1] = dal.ZonePerLedger, dal.SubPLReversions
	copy(key[2:], ledger)
	binary.BigEndian.PutUint64(key[2+dal.LedgerNameFixedSize:], index)

	return key
}

func TestReadReversionsCanonicalWordSurvivesAlias(t *testing.T) {
	t.Parallel()
	store := createTestStore(t)
	writeReversionWord(t, store, "ledger-a", 0, 1<<1)
	alias := reversionWordKey("ledger-a", 0)
	alias[2+len("ledger-a")+1] = 'x'
	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetBytes(alias, bitset.MarshalWord(1<<2)))
	require.NoError(t, batch.Commit())
	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()
	words, malformed, err := query.ReadReversions(handle)
	require.NoError(t, err)
	require.Len(t, malformed, 1)
	require.Contains(t, malformed[0].Reason, "noncanonical key")
	require.True(t, words["ledger-a"].Test(1))
	require.False(t, words["ledger-a"].Test(2), "the alias must not replace the canonical word")
}

// collectReversionEvents runs compareReversions against the store's persisted
// bitsets with the given audit-derived set and returns the REVERTED_MISMATCH
// errors.
func collectReversionEvents(t *testing.T, store *dal.Store, derived map[string]*bitset.Bitset, knownLedgers map[string]struct{}) []*servicepb.CheckStoreError {
	t.Helper()

	checker := NewChecker(store, attributes.New(), nil, logging.Testing())

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
