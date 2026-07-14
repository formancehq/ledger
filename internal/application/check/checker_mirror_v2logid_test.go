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
// MIRROR_V2LOGID_AHEAD errors.
func collectMirrorV2LogIDEvents(t *testing.T, store *dal.Store, attrs *attributes.Attributes, maxV2 map[string]uint64) []*servicepb.CheckStoreError {
	t.Helper()

	checker := NewChecker(store, attrs, "mirror-v2logid-cluster", nil, nil, logging.Testing())

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	chainBound := newChainBoundState()
	maps.Copy(chainBound.maxMirrorV2LogID, maxV2)

	var got []*servicepb.CheckStoreError

	checker.compareMirrorV2LogID(handle, chainBound, func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok &&
			e.Error.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_MIRROR_V2LOGID_AHEAD {
			got = append(got, e.Error)
		}
	})

	return got
}

// TestCompareMirrorV2LogID_AheadFlagged: stored high-water mark strictly above
// the audited max is the corruption/data-loss direction and must emit exactly
// one MIRROR_V2LOGID_AHEAD event naming the ledger.
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
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_MIRROR_V2LOGID_AHEAD, got[0].GetErrorType())
}

// TestCompareMirrorV2LogID_AtOrBelowNotFlagged: stored == max and stored < max
// are both legitimate (self-healing / legacy) and must NOT be flagged.
func TestCompareMirrorV2LogID_AtOrBelowNotFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()

	// stored == audited max.
	writeBoundaries(t, store, attrs, "at-max", &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1, NextLogId: 1, LastMirrorV2LogId: 7,
	})
	// stored < audited max (behind — legitimate, self-healing).
	writeBoundaries(t, store, attrs, "behind", &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1, NextLogId: 1, LastMirrorV2LogId: 3,
	})

	got := collectMirrorV2LogIDEvents(t, store, attrs, map[string]uint64{"at-max": 7, "behind": 7})

	require.Empty(t, got)
}

// TestCompareMirrorV2LogID_ZeroNotFlagged: a legacy / never-ingested ledger
// (stored last_mirror_v2_log_id == 0) is never flagged, even with no audited
// ingests at all (max == 0) — the no-backfill case.
func TestCompareMirrorV2LogID_ZeroNotFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()

	writeBoundaries(t, store, attrs, "legacy-ledger", &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 42, NextLogId: 42, LastMirrorV2LogId: 0,
	})

	// No entry in the audited-max map for this ledger (max == 0).
	got := collectMirrorV2LogIDEvents(t, store, attrs, map[string]uint64{})

	require.Empty(t, got)
}

// TestCompareMirrorV2LogID_MultiLedgerIsolation: only the ahead ledger is
// flagged; a healthy sibling in the same store is untouched.
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
