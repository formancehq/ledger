package check

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// writeBoundaries persists a LedgerBoundaries row for the given ledger through
// the attribute writer, mirroring how the FSM stores it.
func writeBoundaries(t *testing.T, store *dal.Store, attrs *attributes.Attributes, ledger string, b *raftcmdpb.LedgerBoundaries) {
	t.Helper()

	batch := store.OpenWriteSession()
	_, err := attrs.Boundary.Set(batch, domain.LedgerKey{Name: ledger}.Bytes(), b)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

// collectMirrorV2LogIDEvents runs compareMirrorV2LogID against the store's live
// boundaries with the given audited-max map and returns only the
// MIRROR_V2LOGID_MISMATCH errors. deletedLedgers names ledgers audited as
// deleted (their absent boundary row is legitimate).
func collectMirrorV2LogIDEvents(t *testing.T, store *dal.Store, attrs *attributes.Attributes, maxV2 map[string]uint64, deletedLedgers ...string) []*servicepb.CheckStoreError {
	t.Helper()

	checker := NewChecker(store, attrs, "mirror-v2logid-cluster", nil, logging.Testing())

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	chainBound := newChainBoundState()
	maps.Copy(chainBound.maxMirrorV2LogID, maxV2)

	deletedInReplay := make(map[string]struct{}, len(deletedLedgers))
	for _, name := range deletedLedgers {
		deletedInReplay[name] = struct{}{}
	}

	var got []*servicepb.CheckStoreError

	checker.compareMirrorV2LogID(handle, chainBound, deletedInReplay, func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok &&
			e.Error.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_MIRROR_V2LOGID_MISMATCH {
			got = append(got, e.Error)
		}
	})

	return got
}

// TestCompareMirrorV2LogID_AheadFlagged: stored high-water mark strictly above
// the audited max is corruption (claims a v2 log the audit never recorded) and
// must emit exactly one MIRROR_V2LOGID_MISMATCH event naming the ledger.
func TestCompareMirrorV2LogID_AheadFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()

	writeBoundaries(t, store, attrs, "mirror-ledger", &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 5,
		NextLogId:         5,
		LastMirrorV2LogId: 10, // stored claims v2LogId 10 applied
	})

	// Audit only recorded up to v2LogId 7.
	got := collectMirrorV2LogIDEvents(t, store, attrs, map[string]uint64{"mirror-ledger": 7})

	require.Len(t, got, 1)
	require.Equal(t, "mirror-ledger", got[0].GetLedger())
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_MIRROR_V2LOGID_MISMATCH, got[0].GetErrorType())
}

// TestCompareMirrorV2LogID_BehindFlagged: stored high-water mark BELOW the
// audited max means the persisted projection lost applied ground — under the
// equality check (no backfill leniency) this is now FLAGGED.
func TestCompareMirrorV2LogID_BehindFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()

	writeBoundaries(t, store, attrs, "behind-ledger", &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1, NextLogId: 1, LastMirrorV2LogId: 3,
	})

	got := collectMirrorV2LogIDEvents(t, store, attrs, map[string]uint64{"behind-ledger": 7})

	require.Len(t, got, 1)
	require.Equal(t, "behind-ledger", got[0].GetLedger())
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_MIRROR_V2LOGID_MISMATCH, got[0].GetErrorType())
}

// TestCompareMirrorV2LogID_EqualNotFlagged: stored == audited max is the correct
// at-rest state for a mirror ledger and is not flagged.
func TestCompareMirrorV2LogID_EqualNotFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()

	writeBoundaries(t, store, attrs, "at-max", &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1, NextLogId: 1, LastMirrorV2LogId: 7,
	})

	got := collectMirrorV2LogIDEvents(t, store, attrs, map[string]uint64{"at-max": 7})

	require.Empty(t, got)
}

// TestCompareMirrorV2LogID_NonMirrorLedgerNotFlagged: a regular (never-mirrored)
// ledger has stored 0 and no audited MirrorIngest (max 0) → equal → not flagged.
func TestCompareMirrorV2LogID_NonMirrorLedgerNotFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()

	writeBoundaries(t, store, attrs, "regular-ledger", &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 42, NextLogId: 42, LastMirrorV2LogId: 0,
	})

	// No audited MirrorIngest for this ledger (max == 0).
	got := collectMirrorV2LogIDEvents(t, store, attrs, map[string]uint64{})

	require.Empty(t, got)
}

// TestCompareMirrorV2LogID_CorruptToZeroFlagged: a mirror ledger with audited
// ingests whose stored high-water mark was wiped to 0 must be flagged (0 != max).
func TestCompareMirrorV2LogID_CorruptToZeroFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()

	writeBoundaries(t, store, attrs, "wiped-ledger", &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 5, NextLogId: 5, LastMirrorV2LogId: 0,
	})

	got := collectMirrorV2LogIDEvents(t, store, attrs, map[string]uint64{"wiped-ledger": 4})

	require.Len(t, got, 1)
	require.Equal(t, "wiped-ledger", got[0].GetLedger())
}

// TestCompareMirrorV2LogID_AbsentRowFlagged pins the union-driven comparison
// (NumaryBot checker.go:930): a mirror ledger that has audited MirrorIngest
// orders (auditedMax > 0) but NO stored boundary row must be flagged — treated as
// stored 0 (0 != max). Iterating only existing rows would silently skip it,
// leaving the disappearance of last_mirror_v2_log_id undetected.
func TestCompareMirrorV2LogID_AbsentRowFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()

	// No boundary row written for "gone-mirror", but the audit shows ingests up
	// to v2LogId 5.
	got := collectMirrorV2LogIDEvents(t, store, attrs, map[string]uint64{"gone-mirror": 5})

	require.Len(t, got, 1)
	require.Equal(t, "gone-mirror", got[0].GetLedger())
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_MIRROR_V2LOGID_MISMATCH, got[0].GetErrorType())
}

// TestCompareMirrorV2LogID_DeletedMirrorLedgerNotFlagged pins that a LEGITIMATELY
// DELETED mirror ledger — audited ingests (max > 0), a DeleteLedger log in the
// verified range, and its boundary row removed by WriteSet.Absorb — is NOT
// flagged. Without the deleted-ledger exclusion the absent-row branch would
// false-positive on a healthy deletion (NumaryBot checker.go:979).
func TestCompareMirrorV2LogID_DeletedMirrorLedgerNotFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()

	// Audited ingests up to v2LogId 5, no boundary row (removed on delete), and
	// the ledger is in the audited deleted set.
	got := collectMirrorV2LogIDEvents(t, store, attrs, map[string]uint64{"deleted-mirror": 5}, "deleted-mirror")

	require.Empty(t, got)
}

// TestCompareMirrorV2LogID_AbsentRowNonMirrorNotFlagged: a ledger with neither a
// boundary row nor audited ingests (both 0) is not in scope and not flagged —
// the union defaults both sides to 0.
func TestCompareMirrorV2LogID_AbsentRowNonMirrorNotFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()

	// A present, healthy mirror row alongside no entry at all for any absent
	// non-mirror ledger — nothing spurious is emitted.
	writeBoundaries(t, store, attrs, "healthy", &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1, NextLogId: 1, LastMirrorV2LogId: 4,
	})

	got := collectMirrorV2LogIDEvents(t, store, attrs, map[string]uint64{"healthy": 4})

	require.Empty(t, got)
}

// TestCompareMirrorV2LogID_MultiLedgerIsolation: only the diverging ledger is
// flagged; a healthy (equal) sibling in the same store is untouched.
func TestCompareMirrorV2LogID_MultiLedgerIsolation(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()

	writeBoundaries(t, store, attrs, "healthy", &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1, NextLogId: 1, LastMirrorV2LogId: 4,
	})
	writeBoundaries(t, store, attrs, "corrupt", &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1, NextLogId: 1, LastMirrorV2LogId: 99,
	})

	got := collectMirrorV2LogIDEvents(t, store, attrs, map[string]uint64{"healthy": 4, "corrupt": 5})

	require.Len(t, got, 1)
	require.Equal(t, "corrupt", got[0].GetLedger())
}
