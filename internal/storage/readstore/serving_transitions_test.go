package readstore

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
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

	s.serving.Mark(servingLedger, servingCanonical)
	commitVersionState(t, s, IndexVersionState{CurrentVersion: 2, HighWater: 2})
	require.True(t, s.serving.InFlight(servingLedger, servingCanonical))

	image := t.TempDir()
	require.NoError(t, s.CreateCheckpoint(filepath.Join(image, "readindex")))

	assert.False(t, s.serving.InFlight(servingLedger, servingCanonical), "the checkpoint's flush made the promotion durable")

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

	s.serving.Mark(servingLedger, servingCanonical)
	commitVersionState(t, s, IndexVersionState{CurrentVersion: 2, HighWater: 2})
	require.NoError(t, s.serving.Flush())

	reopened := reopenOnDiskImage(t, s)

	state, present, err := reopened.ReadIndexVersionState(servingLedger, servingCanonical)
	require.NoError(t, err)
	require.True(t, present)
	assert.Equal(t, uint32(2), state.CurrentVersion)
	assert.False(t, reopened.serving.InFlight(servingLedger, servingCanonical), "marks are in-memory; a reopened store has nothing in flight")
}

// Between the commit and the flush the promoted state is visible in Pebble
// but not durable, so readers must refuse the index as building rather than
// serve a binding a kill could take back.
func TestServingTransition_InFlightReadsAsBuildingUntilFlushed(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	s.serving.Mark(servingLedger, servingCanonical)
	commitVersionState(t, s, IndexVersionState{CurrentVersion: 2, HighWater: 2})

	snap := s.NewSnapshot()
	defer func() { _ = snap.Close() }()

	resolved, primed, err := s.PinnedVersionResolver(snap, servingLedger, 10)(servingCanonical)
	require.NoError(t, err)
	assert.True(t, primed, "a record exists — this is building, not removed")
	assert.Zero(t, resolved.Version)

	require.NoError(t, s.serving.Flush())
	assert.False(t, s.serving.InFlight(servingLedger, servingCanonical))

	resolved, primed, err = s.PinnedVersionResolver(snap, servingLedger, 10)(servingCanonical)
	require.NoError(t, err)
	assert.True(t, primed)
	assert.Equal(t, uint32(2), resolved.Version)
}

func TestServingTransition_UnmarkWithdrawsACancelledPromotion(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	commitVersionState(t, s, IndexVersionState{CurrentVersion: 1, PendingVersion: 2, HighWater: 2})

	s.serving.Mark(servingLedger, servingCanonical)
	s.serving.Unmark(servingLedger, servingCanonical)

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

	s.serving.Mark(servingLedger, servingCanonical)
	commitVersionState(t, s, IndexVersionState{CurrentVersion: 2, HighWater: 2})

	s.serving.flush = func() error { return errors.New("disk on fire") }
	require.Error(t, s.serving.Flush())
	assert.True(t, s.serving.InFlight(servingLedger, servingCanonical))

	resolved, primed, err := s.PinnedVersionResolver(s.db, servingLedger, 10)(servingCanonical)
	require.NoError(t, err)
	assert.True(t, primed)
	assert.Zero(t, resolved.Version, "still refused: the committed promotion is not on disk")

	s.serving.flush = s.db.Flush
	require.NoError(t, s.serving.Flush())
	assert.False(t, s.serving.InFlight(servingLedger, servingCanonical))
}

// A promotion committed but not yet flushed keeps its gate even when a later
// promotion for the same index is staged and then withdrawn: the mark is a
// count, so the withdrawal releases only the later one.
func TestServingTransitions_WithdrawingALaterMarkKeepsTheEarlierOne(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	st := s.serving

	st.Mark(servingLedger, servingCanonical)
	commitVersionState(t, s, IndexVersionState{CurrentVersion: 2, HighWater: 2})

	s.serving.flush = func() error { return errors.New("disk on fire") }
	require.Error(t, st.Flush())

	st.Mark(servingLedger, servingCanonical)
	st.Unmark(servingLedger, servingCanonical)
	assert.True(t, st.InFlight(servingLedger, servingCanonical), "the committed promotion's mark must survive the later withdrawal")

	s.serving.flush = s.db.Flush
	require.NoError(t, st.Flush())
	assert.False(t, st.InFlight(servingLedger, servingCanonical))
}

// A mark added while a flush is running belongs to the memtable that flush
// did not cover, so the flush must release only the marks it captured.
func TestServingTransitions_FlushReleasesOnlyTheMarksItCaptured(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	st := s.serving

	st.Mark(servingLedger, servingCanonical)
	st.flush = func() error {
		st.Mark(servingLedger, servingCanonical)
		st.Mark(servingLedger, "metadata:account:other")

		return nil
	}

	require.NoError(t, st.Flush())
	assert.True(t, st.InFlight(servingLedger, servingCanonical), "the mark added mid-flush survives")
	assert.True(t, st.InFlight(servingLedger, "metadata:account:other"))

	s.serving.flush = s.db.Flush
	require.NoError(t, st.Flush())
	assert.False(t, st.InFlight(servingLedger, servingCanonical))
	assert.False(t, st.InFlight(servingLedger, "metadata:account:other"))
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

	s.serving.Mark(servingLedger, servingCanonical)
	require.NoError(t, s.serving.Flush())
	assert.Equal(t, before+1, s.db.Metrics().Flush.Count)
}
