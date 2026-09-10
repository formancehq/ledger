package readstore

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

const (
	servingLedger    = "ledger-a"
	servingCanonical = "metadata:account:grade"
)

// reopenOnDiskImage opens what the store holds on disk right now. A Pebble
// checkpoint copies the current version's SSTables and manifest under the DB
// lock, and with the WAL disabled it carries no memtable — so it is exactly
// what a hard kill at this instant would leave behind, with none of the
// inconsistency a plain directory copy risks against a background compaction.
// It calls pebble.DB.Checkpoint directly: Store.CreateCheckpoint flushes
// first and would hide an unflushed promotion from these tests.
func reopenOnDiskImage(t *testing.T, s *Store) *Store {
	t.Helper()

	image := t.TempDir()
	require.NoError(t, s.db.Checkpoint(filepath.Join(image, "readindex")))

	reopened, err := New(image, logging.NopZap(), DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	return reopened
}

func commitVersionState(t *testing.T, s *Store, state IndexVersionState) {
	t.Helper()

	batch := s.NewBatch()
	require.NoError(t, s.WriteIndexVersionState(batch, servingLedger, servingCanonical, state))
	require.NoError(t, batch.Commit())
}

// A committed promotion is not durable on its own: the store has no WAL, so a
// kill before the next flush reopens at the previous state. This is the
// failure the promotion flush exists to close.
func TestServingTransition_UnflushedPromotionDoesNotSurviveAKill(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	commitVersionState(t, s, IndexVersionState{CurrentVersion: 1, PendingVersion: 2, HighWater: 2})
	require.NoError(t, s.db.Flush())

	commitVersionState(t, s, IndexVersionState{CurrentVersion: 2, HighWater: 2})

	reopened := reopenOnDiskImage(t, s)

	state, present, err := reopened.ReadIndexVersionState(servingLedger, servingCanonical)
	require.NoError(t, err)
	require.True(t, present)
	assert.Equal(t, uint32(1), state.CurrentVersion, "the unflushed promotion must be gone after a kill — this is the premise the flush protects against")
}

// CreateCheckpoint flushes the memtable, which makes an in-flight promotion
// durable: the checkpoint carries it and the live store stops refusing it.
func TestServingTransition_CheckpointFlushLiftsTheMark(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	commitVersionState(t, s, IndexVersionState{CurrentVersion: 1, PendingVersion: 2, HighWater: 2})
	require.NoError(t, s.db.Flush())

	s.serving.Mark(servingLedger, servingCanonical, 2)
	commitVersionState(t, s, IndexVersionState{CurrentVersion: 2, HighWater: 2})
	require.True(t, s.serving.InFlight(servingLedger, servingCanonical, 2))

	image := t.TempDir()
	require.NoError(t, s.CreateCheckpoint(filepath.Join(image, "readindex")))

	assert.False(t, s.serving.InFlight(servingLedger, servingCanonical, 2), "the checkpoint's flush made the promotion durable")

	reopened, err := New(image, logging.NopZap(), DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	state, present, err := reopened.ReadIndexVersionState(servingLedger, servingCanonical)
	require.NoError(t, err)
	require.True(t, present)
	assert.Equal(t, uint32(2), state.CurrentVersion, "the checkpoint carries the flushed promotion")
}

func TestServingTransition_FlushedPromotionSurvivesAKill(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	commitVersionState(t, s, IndexVersionState{CurrentVersion: 1, PendingVersion: 2, HighWater: 2})
	require.NoError(t, s.db.Flush())

	s.serving.Mark(servingLedger, servingCanonical, 2)
	commitVersionState(t, s, IndexVersionState{CurrentVersion: 2, HighWater: 2})
	require.NoError(t, s.serving.Flush())

	reopened := reopenOnDiskImage(t, s)

	state, present, err := reopened.ReadIndexVersionState(servingLedger, servingCanonical)
	require.NoError(t, err)
	require.True(t, present)
	assert.Equal(t, uint32(2), state.CurrentVersion)
	assert.False(t, reopened.serving.InFlight(servingLedger, servingCanonical, 2), "marks are in-memory; a reopened store has nothing in flight")
}

// Between the commit and the flush the promoted state is visible in Pebble
// but not durable. An initial build retains nothing, so readers must refuse
// the index as building rather than serve a binding a kill could take back.
func TestServingTransition_InFlightInitialBuildReadsAsBuildingUntilFlushed(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	s.serving.Mark(servingLedger, servingCanonical, 2)
	commitVersionState(t, s, IndexVersionState{CurrentVersion: 2, HighWater: 2})

	snap := s.NewSnapshot()
	defer func() { _ = snap.Close() }()

	resolved, primed, err := s.PinnedVersionResolver(snap, servingLedger, 10)(servingCanonical)
	require.NoError(t, err)
	assert.True(t, primed, "a record exists — this is building, not removed")
	assert.Zero(t, resolved.Version)

	require.NoError(t, s.serving.Flush())
	assert.False(t, s.serving.InFlight(servingLedger, servingCanonical, 2))

	resolved, primed, err = s.PinnedVersionResolver(snap, servingLedger, 10)(servingCanonical)
	require.NoError(t, err)
	assert.True(t, primed)
	assert.Equal(t, uint32(2), resolved.Version)
}

// retypedInFlight is the state a rewrite switch commits: v2 current under its
// new binding, v1 retained under the old one, activation at 8 and the
// replaced keyspace complete through log 12.
func retypedInFlight() IndexVersionState {
	return IndexVersionState{
		CurrentVersion:             2,
		HighWater:                  2,
		ActivationSequence:         8,
		CurrentType:                commonpb.MetadataType_METADATA_TYPE_INT64,
		CurrentTypeDeclared:        true,
		PreviousVersion:            1,
		PreviousType:               commonpb.MetadataType_METADATA_TYPE_STRING,
		PreviousTypeDeclared:       true,
		PreviousActivationSequence: 3,
		PreviousValidThrough:       12,
	}
}

// A retype retains the replaced version, so while the promotion is in flight
// readers are served from it under its own binding instead of refused; the
// flush switches them to the promoted version.
func TestServingTransition_InFlightRetypeServesTheRetainedVersion(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	s.serving.Mark(servingLedger, servingCanonical, 2)
	commitVersionState(t, s, retypedInFlight())

	snap := s.NewSnapshot()
	defer func() { _ = snap.Close() }()

	resolved, primed, err := s.PinnedVersionResolver(snap, servingLedger, 10)(servingCanonical)
	require.NoError(t, err)
	require.True(t, primed)
	assert.Equal(t, uint32(1), resolved.Version, "served from the retained version while the promotion is unflushed")
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_STRING, resolved.Type, "under the retained version's own binding")
	assert.True(t, resolved.TypeDeclared)
	assert.True(t, resolved.BindingKnown)

	require.NoError(t, s.serving.Flush())

	resolved, _, err = s.PinnedVersionResolver(snap, servingLedger, 10)(servingCanonical)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), resolved.Version)
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, resolved.Type)
}

// The retained version is only good for the logs its keyspace received. A
// pin past that, or below its own activation, is refused; and a retained
// version whose own promotion is still in flight was never flushed either.
func TestServingTransition_RetainedVersionBounds(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	s.serving.Mark(servingLedger, servingCanonical, 2)
	commitVersionState(t, s, retypedInFlight())

	resolve := func(pin uint64) uint32 {
		resolved, primed, err := s.PinnedVersionResolver(s.db, servingLedger, pin)(servingCanonical)
		require.NoError(t, err)
		require.True(t, primed)

		return resolved.Version
	}

	assert.Equal(t, uint32(1), resolve(12), "the last log the retained keyspace received")
	assert.Zero(t, resolve(13), "past it the retained keyspace is incomplete: refused")
	assert.Equal(t, uint32(1), resolve(3), "at the retained version's own activation")
	assert.Zero(t, resolve(2), "below it the retained keyspace resolves empty: refused")
	assert.Zero(t, resolve(0), "no pin: nothing bounds a read to the retained keyspace, refused")

	s.serving.Mark(servingLedger, servingCanonical, 1)
	assert.Zero(t, resolve(10), "a retained version that is itself unflushed is refused")
}

// A pin below the promoted version's activation cannot use it; while the
// replaced version is retained it is served instead, at any point of the
// promotion's life.
func TestServingTransition_PinBelowActivationServesTheRetainedVersion(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	commitVersionState(t, s, retypedInFlight())

	resolved, primed, err := s.PinnedVersionResolver(s.db, servingLedger, 7)(servingCanonical)
	require.NoError(t, err)
	require.True(t, primed)
	assert.Equal(t, uint32(1), resolved.Version, "pinned below v2's activation, served from v1")

	resolved, _, err = s.PinnedVersionResolver(s.db, servingLedger, 8)(servingCanonical)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), resolved.Version, "at the activation v2 serves")

	retired := retypedInFlight()
	retired.PreviousVersion = 0
	commitVersionState(t, s, retired)

	resolved, primed, err = s.PinnedVersionResolver(s.db, servingLedger, 7)(servingCanonical)
	require.NoError(t, err)
	require.True(t, primed)
	assert.Zero(t, resolved.Version, "once retired there is nothing to fall back to")
}

// Marks name the version they promote, so a snapshot that predates the commit
// still holds the standing version and keeps serving it.
func TestServingTransition_SnapshotBeforeTheCommitIsNotRefused(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	commitVersionState(t, s, IndexVersionState{CurrentVersion: 1, PendingVersion: 2, HighWater: 2})

	snap := s.NewSnapshot()
	defer func() { _ = snap.Close() }()

	s.serving.Mark(servingLedger, servingCanonical, 2)
	commitVersionState(t, s, IndexVersionState{CurrentVersion: 2, HighWater: 2})

	resolved, primed, err := s.PinnedVersionResolver(snap, servingLedger, 10)(servingCanonical)
	require.NoError(t, err)
	require.True(t, primed)
	assert.Equal(t, uint32(1), resolved.Version, "the snapshot's current version is not the one in flight")
}

func TestServingTransition_UnmarkWithdrawsACancelledPromotion(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	commitVersionState(t, s, IndexVersionState{CurrentVersion: 1, PendingVersion: 2, HighWater: 2})

	s.serving.Mark(servingLedger, servingCanonical, 2)
	s.serving.Unmark(servingLedger, servingCanonical, 2)

	resolved, primed, err := s.PinnedVersionResolver(s.db, servingLedger, 10)(servingCanonical)
	require.NoError(t, err)
	assert.True(t, primed)
	assert.Equal(t, uint32(1), resolved.Version, "the standing state serves once the abandoned mark is gone")
}

// A failed flush leaves every mark in place: the promotion is committed but
// not durable, so readers must keep refusing until a later flush succeeds.
func TestServingTransitions_FlushFailureKeepsTheMarks(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	s.serving.Mark(servingLedger, servingCanonical, 2)
	commitVersionState(t, s, IndexVersionState{CurrentVersion: 2, HighWater: 2})

	s.serving.flush = func() error { return errors.New("disk on fire") }
	require.Error(t, s.serving.Flush())
	assert.True(t, s.serving.InFlight(servingLedger, servingCanonical, 2))

	resolved, primed, err := s.PinnedVersionResolver(s.db, servingLedger, 10)(servingCanonical)
	require.NoError(t, err)
	assert.True(t, primed)
	assert.Zero(t, resolved.Version, "still refused: the committed promotion is not on disk")

	s.serving.flush = s.db.Flush
	require.NoError(t, s.serving.Flush())
	assert.False(t, s.serving.InFlight(servingLedger, servingCanonical, 2))
}

// A promotion committed but not yet flushed keeps its gate even when a later
// promotion for the same index is staged and then withdrawn: the mark is a
// count, so the withdrawal releases only the later one.
func TestServingTransitions_WithdrawingALaterMarkKeepsTheEarlierOne(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	st := s.serving

	st.Mark(servingLedger, servingCanonical, 2)
	commitVersionState(t, s, IndexVersionState{CurrentVersion: 2, HighWater: 2})

	s.serving.flush = func() error { return errors.New("disk on fire") }
	require.Error(t, st.Flush())

	st.Mark(servingLedger, servingCanonical, 2)
	st.Unmark(servingLedger, servingCanonical, 2)
	assert.True(t, st.InFlight(servingLedger, servingCanonical, 2), "the committed promotion's mark must survive the later withdrawal")

	s.serving.flush = s.db.Flush
	require.NoError(t, st.Flush())
	assert.False(t, st.InFlight(servingLedger, servingCanonical, 2))
}

// A mark added while a flush is running belongs to the memtable that flush
// did not cover, so the flush must release only the marks it captured.
func TestServingTransitions_FlushReleasesOnlyTheMarksItCaptured(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	st := s.serving

	st.Mark(servingLedger, servingCanonical, 2)
	st.flush = func() error {
		st.Mark(servingLedger, servingCanonical, 2)
		st.Mark(servingLedger, "metadata:account:other", 1)

		return nil
	}

	require.NoError(t, st.Flush())
	assert.True(t, st.InFlight(servingLedger, servingCanonical, 2), "the mark added mid-flush survives")
	assert.True(t, st.InFlight(servingLedger, "metadata:account:other", 1))

	s.serving.flush = s.db.Flush
	require.NoError(t, st.Flush())
	assert.False(t, st.InFlight(servingLedger, servingCanonical, 2))
	assert.False(t, st.InFlight(servingLedger, "metadata:account:other", 1))
}

// Ordinary fold commits carry no serving transition and must not pay for a
// flush; only a marked transition triggers one.
func TestServingTransitions_FlushesOnlyWhenMarked(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	commitVersionState(t, s, IndexVersionState{CurrentVersion: 1, HighWater: 1})

	before := s.db.Metrics().Flush.Count
	require.NoError(t, s.serving.Flush())
	assert.Equal(t, before, s.db.Metrics().Flush.Count, "nothing marked, nothing flushed")

	s.serving.Mark(servingLedger, servingCanonical, 1)
	require.NoError(t, s.serving.Flush())
	assert.Equal(t, before+1, s.db.Metrics().Flush.Count)
}

// The retained fields round-trip through the encoding.
func TestIndexVersionState_RetainedFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	want := retypedInFlight()
	want.RewriteProgress = []byte{7, 7}
	commitVersionState(t, s, want)

	got, present, err := s.ReadIndexVersionState(servingLedger, servingCanonical)
	require.NoError(t, err)
	require.True(t, present)
	assert.Equal(t, want, got)
}

// AnyLiveIn sees reservations and pins alike, at the sequence each was taken
// at, and reports nothing once every lease is released.
func TestLeaseRegistry_AnyLiveIn(t *testing.T) {
	t.Parallel()

	r := NewLeaseRegistry()

	assert.False(t, r.AnyLiveIn(0, 100))

	hold := r.Reserve(5)
	pinned, ok := r.Pin(9)
	require.True(t, ok)

	assert.True(t, r.AnyLiveIn(5, 6), "the reservation counts at its reserved sequence")
	assert.True(t, r.AnyLiveIn(9, 10))
	assert.False(t, r.AnyLiveIn(6, 9), "the range is [lo, hi)")
	assert.False(t, r.AnyLiveIn(10, 100))

	hold.Release()
	assert.False(t, r.AnyLiveIn(0, 9))
	assert.True(t, r.AnyLiveIn(0, 10))

	pinned.Release()
	assert.False(t, r.AnyLiveIn(0, 100))
}
