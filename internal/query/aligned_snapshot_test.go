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
			// Simulated fold catch-up while the query waits.
			time.Sleep(20 * time.Millisecond)
			setReadStoreProgress(t, rs, lastSeq)
		}()

		snap, mainSeq, release, err := query.AlignedIndexSnapshot(t.Context(), rs, handle)
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

		snap, mainSeq, release, err := query.AlignedIndexSnapshot(t.Context(), rs, handle)
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
	_, _, _, err = query.AlignedIndexSnapshot(ctx, rs, handle)

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

// A pin the event GC has already reclaimed past cannot be salvaged in place:
// mainReader is a fixed snapshot, so re-reading it returns the same rejected
// sequence. The read must fail immediately so a retry can open a fresh handle.
func TestAlignedIndexSnapshot_ReclaimedPinFailsFast(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	registerLedger(t, store, "l")
	appendLogs(t, store, 3, createTestLogsForLedger("l", 1)...)

	rs := newTestReadStore(t)

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	lastSeq, err := query.ReadLastSequence(handle)
	require.NoError(t, err)

	setReadStoreProgress(t, rs, lastSeq)

	// A GC pass sweeps past the handle's sequence while no lease holds it down.
	rs.Leases().BeginGC(lastSeq + 1)

	start := time.Now()
	_, _, _, err = query.AlignedIndexSnapshot(t.Context(), rs, handle)

	var reclaimed *query.ErrReadPinReclaimed
	require.ErrorAs(t, err, &reclaimed)
	require.Equal(t, lastSeq, reclaimed.Pin)
	require.Less(t, time.Since(start), time.Second, "must not spin against the frozen handle until the deadline")
}
