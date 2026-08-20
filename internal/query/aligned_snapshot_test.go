package query_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func newTestReadStore(t *testing.T) *readstore.Store {
	t.Helper()

	rs, err := readstore.New(t.TempDir(), logging.NopZap(), readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })

	return rs
}

func setReadStoreProgress(t *testing.T, rs *readstore.Store, seq uint64) {
	t.Helper()

	batch := rs.NewBatch()
	require.NoError(t, rs.WriteProgress(batch, seq))
	require.NoError(t, batch.Commit())
	rs.NotifyProgress()
}

// The alignment invariant: the returned snapshot's fold cursor covers the
// main handle's last applied sequence, so index leaves can never lag the
// main-store leaves of the same query (EN-1748).
func TestAlignedIndexSnapshot(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	registerLedger(t, store, "l")
	appendLogs(t, store, 3, createTestLogsForLedger("l", 1)...) // sequences 1..3

	rs := newTestReadStore(t)

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	lastSeq, err := query.ReadLastSequence(handle)
	require.NoError(t, err)
	require.Positive(t, lastSeq)

	t.Run("behind then catches up", func(t *testing.T) {
		setReadStoreProgress(t, rs, lastSeq-1)

		done := make(chan struct{})
		go func() {
			defer close(done)
			// Concurrent fold catch-up. The write deliberately races the
			// aligned call: landing before it exercises the fast path, after
			// it the wait-and-retake loop — both legal executions of the
			// invariant. The never-catches-up case is pinned by the
			// context-bound test below.
			setReadStoreProgress(t, rs, lastSeq)
		}()

		snap, mainSeq, release, err := query.AlignedIndexSnapshot(t.Context(), rs, handle, func() {})
		require.NoError(t, err)
		defer release()
		defer func() { _ = snap.Close() }()
		<-done

		require.Equal(t, lastSeq, mainSeq)

		// The vouched cursor is readable through the snapshot itself.
		got, err := rs.LastIndexedSequenceFrom(snap)
		require.NoError(t, err)
		require.GreaterOrEqual(t, got, mainSeq)
	})

	t.Run("already aligned", func(t *testing.T) {
		setReadStoreProgress(t, rs, lastSeq+4)

		snap, mainSeq, release, err := query.AlignedIndexSnapshot(t.Context(), rs, handle, func() {})
		require.NoError(t, err)
		defer release()
		defer func() { _ = snap.Close() }()

		require.Equal(t, lastSeq, mainSeq)
	})
}

// Alignment waits for as long as the caller allows and no longer: the bound
// is the caller's context, never a server-chosen constant. A cap would also
// diverge — mainSeq is fixed for this handle, so waiting converges, while a
// retry re-pins higher and leaves the fold further behind.
func TestAlignedIndexSnapshot_WaitsOnlyAsLongAsTheCallerAllows(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	registerLedger(t, store, "l")
	appendLogs(t, store, 3, createTestLogsForLedger("l", 1)...)

	rs := newTestReadStore(t)
	setReadStoreProgress(t, rs, 1) // permanently behind

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, _, _, err = query.AlignedIndexSnapshot(ctx, rs, handle, func() {})

	require.ErrorIs(t, err, context.DeadlineExceeded, "the caller's deadline is what ends the wait")
	require.Less(t, time.Since(start), time.Second, "it must not outlive the caller's deadline")
}

// A committed transaction whose index rows have not folded yet must be
// admitted by the horizon (it exists in the main store); an id beyond the
// handle must not.
func TestMainHorizonKeep_Transactions(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	registerLedger(t, store, "l")

	rs := newTestReadStore(t)
	setReadStoreProgress(t, rs, 5)

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	snap := rs.NewSnapshot()
	defer func() { _ = snap.Close() }()

	keep := query.MainHorizonKeep(commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, handle, snap, "l", 5)
	require.NotNil(t, keep)

	ok, err := keep(txIDBytesQ(1))
	require.NoError(t, err)
	require.False(t, ok, "no transaction rows in the main store at all")

	_, err = keep([]byte("short"))
	require.Error(t, err, "malformed entity is a loud error, never admitted")
}

func txIDBytesQ(id uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, id)

	return b
}

// OpenQueryHandle holds the reclaim floor across the handle's creation, so
// the pin it yields can never sit beneath it — the refusal path inside
// AlignedIndexSnapshot is an invariant guard, not a condition reads meet.
func TestOpenQueryHandle_PinSurvivesAConcurrentSweep(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	registerLedger(t, store, "l")
	appendLogs(t, store, 3, createTestLogsForLedger("l", 1)...)

	rs := newTestReadStore(t)

	// Any filtered shape is owed alignment, so the handle takes a reservation.
	filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{
		Field: &commonpb.FieldCondition{
			Field:     &commonpb.FieldRef{Metadata: "tier"},
			Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{}},
		},
	}}

	handle, releaseHold, err := query.OpenQueryHandle(rs, store, filter, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	require.NoError(t, err)

	defer releaseHold()
	defer func() { _ = handle.Close() }()

	lastSeq, err := query.ReadLastSequence(handle)
	require.NoError(t, err)
	setReadStoreProgress(t, rs, lastSeq)

	// A sweep proposing a watermark far past the handle cannot pass it: the
	// reservation taken before the handle holds the floor down.
	require.Equal(t, uint64(0), rs.Leases().BeginGC(lastSeq+1_000),
		"the reservation pins the floor at its pre-handle value")

	snap, mainSeq, releaseLease, err := query.AlignedIndexSnapshot(t.Context(), rs, handle, func() {})
	require.NoError(t, err, "a handle from OpenQueryHandle must always be admissible")

	defer releaseLease()
	defer func() { _ = snap.Close() }()

	require.Equal(t, lastSeq, mainSeq)
}

// The reservation is what makes the later Acquire un-refusable, so a read
// that never acquires must not take one: holding the floor for the life of a
// request — or of a streaming cursor — would stall event reclamation in
// exchange for nothing.
func TestOpenQueryHandle_UnalignedReadHoldsNoFloor(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	registerLedger(t, store, "l")
	appendLogs(t, store, 3, createTestLogsForLedger("l", 1)...)

	rs := newTestReadStore(t)

	handle, releaseHold, err := query.OpenQueryHandle(rs, store, nil, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	require.NoError(t, err)

	defer releaseHold()
	defer func() { _ = handle.Close() }()

	require.Equal(t, uint64(500), rs.Leases().BeginGC(500),
		"an unaligned read must not hold the event GC back")
}

// The reservation covers the window before the read's pin exists, and no
// longer. Once alignment has registered that pin, the read's own lease
// retains everything it needs, so the GC watermark must be free to rise to
// it — otherwise every request in flight drags the watermark down to the fold
// cursor as of its own start, and the floor advances only in a moment when no
// aligned read is running at all.
func TestAlignedIndexSnapshot_ReleasesTheReservationOnceThePinExists(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	registerLedger(t, store, "l")
	appendLogs(t, store, 3, createTestLogsForLedger("l", 1)...)

	rs := newTestReadStore(t)
	setReadStoreProgress(t, rs, 0)

	filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{
		Field: &commonpb.FieldCondition{
			Field:     &commonpb.FieldRef{Metadata: "tier"},
			Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{}},
		},
	}}

	// Reserved while the fold is still at 0, so the reservation sits well
	// below the pin the read ends up with.
	handle, releaseHold, err := query.OpenQueryHandle(rs, store, filter, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	require.NoError(t, err)

	defer releaseHold()
	defer func() { _ = handle.Close() }()

	lastSeq, err := query.ReadLastSequence(handle)
	require.NoError(t, err)
	setReadStoreProgress(t, rs, lastSeq)

	snap, _, releaseLease, err := query.AlignedIndexSnapshot(t.Context(), rs, handle, releaseHold)
	require.NoError(t, err)

	defer releaseLease()
	defer func() { _ = snap.Close() }()

	require.Equal(t, lastSeq, rs.Leases().BeginGC(lastSeq+1_000),
		"the read's pin is the only thing still held, so the watermark rises to it")
}

// The reservation is taken at the fold cursor, not at the floor: a read
// parked on a lagging fold must not pin reclamation to the bottom of retained
// history for the length of its wait.
func TestOpenQueryHandle_ReservesAtTheFoldCursor(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	registerLedger(t, store, "l")
	appendLogs(t, store, 3, createTestLogsForLedger("l", 1)...)

	rs := newTestReadStore(t)
	setReadStoreProgress(t, rs, 2)

	filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{
		Field: &commonpb.FieldCondition{
			Field:     &commonpb.FieldRef{Metadata: "tier"},
			Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{}},
		},
	}}

	handle, releaseHold, err := query.OpenQueryHandle(rs, store, filter, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	require.NoError(t, err)

	defer releaseHold()
	defer func() { _ = handle.Close() }()

	// Before alignment runs: the only lease is the reservation.
	require.Equal(t, uint64(2), rs.Leases().BeginGC(1_000),
		"everything below the fold cursor stays reclaimable while the read waits")
}
