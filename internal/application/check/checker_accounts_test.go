package check

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// writeAccountMarker writes a single SubAttrAccount marker directly to Pebble.
func writeAccountMarker(t *testing.T, engine *testEngine, ledger, account string) {
	t.Helper()

	key := domain.AccountKey{LedgerName: ledger, Account: account}
	batch := engine.store.OpenWriteSession()
	_, err := engine.attrs.Account.Set(batch, key.Bytes(), &commonpb.AccountState{})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

// writeVolumeForAccount writes a volume attribute for an account so the
// forward check's referenced set includes the account.
func writeVolumeForAccount(t *testing.T, engine *testEngine, ledger, account, asset string) {
	t.Helper()

	key := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: ledger, Account: account},
		Asset:      asset,
	}
	batch := engine.store.OpenWriteSession()
	_, err := engine.attrs.Volume.Set(batch, key.Bytes(), &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(100),
		Output: commonpb.NewUint256FromUint64(0),
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

// collectAccountMismatchErrors runs a full checker and returns only
// CHECK_STORE_ERROR_TYPE_ACCOUNT_MISMATCH events.
func collectAccountMismatchErrors(t *testing.T, engine *testEngine) []*servicepb.CheckStoreError {
	t.Helper()

	var mismatch []*servicepb.CheckStoreError

	for _, e := range collectCheckErrors(t, engine.store, engine.attrs) {
		if e.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_ACCOUNT_MISMATCH {
			mismatch = append(mismatch, e)
		}
	}

	return mismatch
}

// TestCompareAccounts_HappyPath verifies that a live marker that is referenced
// by a live volume (and a replay touch for the reverse check) produces ZERO
// ACCOUNT_MISMATCH events.
func TestCompareAccounts_HappyPath(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	// Create the minimal store backbone so Check doesn't short-circuit at
	// lastSequence == 0 (needs at least one committed log).
	engine.processAndCommit(createLedgerOrder("test"))

	// Give the account a live volume so it appears in the referenced set.
	writeVolumeForAccount(t, engine, "test", "users:alice", "USD")

	// Write the live account marker.
	writeAccountMarker(t, engine, "test", "users:alice")

	// Use the full checker (processAndCommit drove the audit + logs).
	// The replay-touched set for "test" is empty (no account-type defaults are
	// wired in this engine run), so the reverse check won't fire.
	// The forward check finds the marker referenced by the live volume → OK.
	errs := collectAccountMismatchErrors(t, engine)
	require.Empty(t, errs, "a valid marker with a live volume reference must not trigger ACCOUNT_MISMATCH")
}

// TestCompareAccounts_OrphanMarker_ForwardCheck verifies that a live
// SubAttrAccount marker for an account that has no corresponding volume and
// no replay-recorded touch emits exactly ONE ACCOUNT_MISMATCH event (orphan
// marker — could be forged state).
func TestCompareAccounts_OrphanMarker_ForwardCheck(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	// At least one log so Check doesn't exit early.
	engine.processAndCommit(createLedgerOrder("test"))

	// Write an orphan marker: no volume, no replay touch for this account.
	writeAccountMarker(t, engine, "test", "vendors:orphan")

	errs := collectAccountMismatchErrors(t, engine)
	require.Len(t, errs, 1, "orphan account marker (no volume, no replay touch) must produce exactly one ACCOUNT_MISMATCH")
	require.Equal(t, "test", errs[0].GetLedger())
	require.Equal(t, "vendors:orphan", errs[0].GetAccount())
}

// TestCompareAccounts_MissingMarker_ReverseCheck verifies that an account
// recorded as touched in the replay store (markers are now recorded for every
// account touch) but with NO live marker emits exactly ONE ACCOUNT_MISMATCH.
//
// This uses the compareAccounts method directly with a hand-crafted replayStore
// so we can seed a replay touch without requiring a real processAndCommit cycle.
func TestCompareAccounts_MissingMarker_ReverseCheck(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()

	checker := NewChecker(store, attrs, "test-cluster", nil)

	// Build a replayStore with a single account touch but no live marker.
	rs := newTestReplayStore(t)
	key := domain.AccountKey{LedgerName: "test", Account: "users:bob"}
	require.NoError(t, rs.RecordAccount(key.Bytes(), &commonpb.Timestamp{Data: 1}))

	// compareAccounts needs a PebbleReader: use a snapshot from the store.
	snap, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = snap.Close() })

	var events []*servicepb.CheckStoreError
	callback := func(e *servicepb.CheckStoreEvent) {
		if errEvt := e.GetError(); errEvt != nil {
			events = append(events, errEvt)
		}
	}

	n := checker.compareAccounts(snap, nil, rs, callback)

	require.Equal(t, 1, n, "compareAccounts must return 1 for the missing marker")
	require.Len(t, events, 1)
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_ACCOUNT_MISMATCH, events[0].GetErrorType())
	require.Equal(t, "test", events[0].GetLedger())
	require.Equal(t, "users:bob", events[0].GetAccount())
}

// TestCompareAccounts_TouchedAccountWithMarker_NoError verifies the reverse
// check is silent when a replay-touched account has a matching live marker.
func TestCompareAccounts_TouchedAccountWithMarker_NoError(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()

	checker := NewChecker(store, attrs, "test-cluster", nil)

	// Write the live marker to Pebble.
	key := domain.AccountKey{LedgerName: "test", Account: "users:carol"}
	batch := store.OpenWriteSession()
	_, err := attrs.Account.Set(batch, key.Bytes(), &commonpb.AccountState{})
	require.NoError(t, err)
	// Also write a volume so the forward check also passes.
	volKey := domain.VolumeKey{AccountKey: key, Asset: "USD"}
	_, err = attrs.Volume.Set(batch, volKey.Bytes(), &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(50),
		Output: commonpb.NewUint256FromUint64(0),
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Record the account as replay-touched.
	rs := newTestReplayStore(t)
	require.NoError(t, rs.RecordAccount(key.Bytes(), &commonpb.Timestamp{Data: 1}))

	snap, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = snap.Close() })

	var events []*servicepb.CheckStoreError
	n := checker.compareAccounts(snap, nil, rs, func(e *servicepb.CheckStoreEvent) {
		if errEvt := e.GetError(); errEvt != nil {
			events = append(events, errEvt)
		}
	})

	require.Equal(t, 0, n, "a touched account with a live marker must produce no errors")
	require.Empty(t, events)
}

// TestCompareAccounts_BaselineMarkerReferenced_NoFalsePositive is the
// regression test for the archive-recovery false positive (PR #564, finding
// [2]). A live marker whose account has NO live volume and NO replay touch —
// but IS present in the archived (baseline) checkpoint — must NOT be flagged:
// a marker captured by the baseline is audit-derived and legitimately
// referenced (its volume may have been purged and its type later removed, yet
// markers are never deleted, invariant #5). Without folding baseline markers
// into the referenced set, this would emit a spurious ACCOUNT_MISMATCH on a
// healthy ledger.
func TestCompareAccounts_BaselineMarkerReferenced_NoFalsePositive(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()
	checker := NewChecker(store, attrs, "test-cluster", nil)

	// Live marker for an account with no live volume and no replay touch — its
	// only justification is the baseline marker written below.
	key := domain.AccountKey{LedgerName: "test", Account: "ephemeral:gone"}
	batch := store.OpenWriteSession()
	_, err := attrs.Account.Set(batch, key.Bytes(), &commonpb.AccountState{})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Baseline checkpoint (a separate Pebble) holding the SAME marker, as an
	// archived snapshot would.
	baseDB, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = baseDB.Close() })

	baseSession := dal.NewWriteSessionFromDB(baseDB)
	_, err = attrs.Account.Set(baseSession, key.Bytes(), &commonpb.AccountState{})
	require.NoError(t, err)
	require.NoError(t, baseSession.Commit())

	snap, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = snap.Close() })

	rs := newTestReplayStore(t) // empty: no replay touch for the account

	var events []*servicepb.CheckStoreError
	n := checker.compareAccounts(snap, baseDB, rs, func(e *servicepb.CheckStoreEvent) {
		if errEvt := e.GetError(); errEvt != nil {
			events = append(events, errEvt)
		}
	})

	require.Equal(t, 0, n, "a live marker referenced only by a baseline marker must not be flagged")
	require.Empty(t, events)
}

// TestCompareAccounts_EndToEnd_DefaultMetadataLedger is an end-to-end test
// that wires up a ledger with account-type defaults through processAndCommit,
// confirms the account marker is written to Pebble (the testEngine is extended
// to flush accounts), and verifies the checker finds no errors.
//
// Since the testEngine currently does not flush account markers to Pebble
// (PutAccount is in-memory only), this test uses addAccountTypeWithDefaultsOrder
// and then writes the marker manually to exercise the full checker pass.
func TestCompareAccounts_EndToEnd_DefaultMetadataLedger(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	// Create a fresh ledger.
	engine.processAndCommit(createLedgerOrder("payments"))

	// Add an account type with default_metadata on the fresh (zero-tx) ledger.
	engine.processAndCommit(addAccountTypeWithDefaultMetadataOrder(
		"payments", "customer", "customers:{id}",
		map[string]string{"tier": "standard"},
	))

	// Create a transaction — this creates account customers:alice for the first
	// time. The FSM (in-memory) writes the marker via PutAccount, but the
	// testEngine doesn't flush it to Pebble. We mimic the real WriteSet by
	// writing it manually here.
	engine.processAndCommit(createTransactionOrder("payments", true,
		newPosting("world", "customers:alice", "USD", 100),
	))

	// Manually flush the account marker to Pebble (mirrors what the real FSM's
	// WriteSet.Merge does via dal.SubAttrAccount).
	writeAccountMarker(t, engine, "payments", "customers:alice")

	// Also write the world volume so the checker doesn't find a rogue volume.
	// (The testEngine does flush volumes, so customers:alice/USD is already there.)

	errs := collectAccountMismatchErrors(t, engine)
	require.Empty(t, errs, "customers:alice must be referenced by its live volume and marker must be present")
}

// TestCompareAccounts_EndToEnd_MetadataSetCreatesAccount verifies the
// metadata-set creation path (EN-1276): a SaveMetadata order that first-creates
// an account (no transaction, hence no volume) must have its existence marker
// recorded by replay, so the checker accepts the live marker. Without replay
// recording the SavedMetadata touch, the marker would be an orphan (no volume,
// no replay touch) and the forward check would false-positive — so this is the
// regression test for the replay SavedMetadata marker branch.
func TestCompareAccounts_EndToEnd_MetadataSetCreatesAccount(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	engine.processAndCommit(createLedgerOrder("payments"))

	// Default-bearing account type on the fresh (zero-tx) ledger.
	engine.processAndCommit(addAccountTypeWithDefaultMetadataOrder(
		"payments", "customer", "customers:{id}",
		map[string]string{"tier": "standard"},
	))

	// Metadata-set first-creates customers:alice — no transaction, so the account
	// has no volume; its only audit reference is this SavedMetadata touch.
	engine.processAndCommit(saveAccountMetadataOrder(
		"payments", "customers:alice", map[string]string{"note": "vip"},
	))

	// Flush the marker the in-memory FSM wrote (testEngine does not flush account
	// markers to Pebble; mirrors the real WriteSet.Merge).
	writeAccountMarker(t, engine, "payments", "customers:alice")

	errs := collectAccountMismatchErrors(t, engine)
	require.Empty(t, errs, "metadata-set-created account: replay records the touch and the live marker satisfies both checks")
}

// addAccountTypeWithDefaultMetadataOrder is a helper (checker-test local) for
// building AddAccountType orders with a DefaultMetadata map from a plain
// string map.
func addAccountTypeWithDefaultMetadataOrder(ledger, name, pattern string, defaults map[string]string) *raftcmdpb.Order {
	dm := make(map[string]*commonpb.MetadataValue, len(defaults))
	for k, v := range defaults {
		dm[k] = commonpb.NewStringValue(v)
	}

	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Data: &raftcmdpb.LedgerApplyOrder_AddAccountType{
							AddAccountType: &raftcmdpb.AddAccountTypeOrder{
								AccountType: &commonpb.AccountType{
									Name:            name,
									Pattern:         pattern,
									DefaultMetadata: dm,
								},
							},
						},
					},
				},
			},
		},
	}
}
