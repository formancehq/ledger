package query_test

import (
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

func TestAlignedIndexSnapshot_TimesOutNotCaughtUp(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	registerLedger(t, store, "l")
	appendLogs(t, store, 3, createTestLogsForLedger("l", 1)...)

	rs := newTestReadStore(t)
	setReadStoreProgress(t, rs, 1) // permanently behind

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	lastSeq, err := query.ReadLastSequence(handle)
	require.NoError(t, err)
	require.Greater(t, lastSeq, uint64(1))

	start := time.Now()
	_, _, _, err = query.AlignedIndexSnapshot(t.Context(), rs, handle)

	var notCaughtUp *query.ErrReadIndexNotCaughtUp
	require.ErrorAs(t, err, &notCaughtUp)
	require.Equal(t, lastSeq, notCaughtUp.Requested)
	require.Equal(t, uint64(1), notCaughtUp.Current)
	require.GreaterOrEqual(t, time.Since(start), time.Second, "the bounded wait ran before failing")
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
