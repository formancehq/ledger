package readstore_test

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestIndexVersionState_RoundTrip(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), discardLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	canonical := "acct:metadata:score"
	state := readstore.IndexVersionState{
		CurrentVersion:  3,
		PendingVersion:  4,
		RewriteProgress: []byte("cursor-bytes"),
	}

	batch := store.NewBatch()
	require.NoError(t, store.WriteIndexVersionState(batch, "ledger1", canonical, state))
	require.NoError(t, batch.Commit())

	got, ok, err := store.ReadIndexVersionState("ledger1", canonical)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, state.CurrentVersion, got.CurrentVersion)
	assert.Equal(t, state.PendingVersion, got.PendingVersion)
	assert.Equal(t, state.RewriteProgress, got.RewriteProgress)
}

func TestIndexVersionState_NoRewriteProgress(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), discardLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	canonical := "tx:metadata:tag"
	state := readstore.IndexVersionState{CurrentVersion: 1}

	batch := store.NewBatch()
	require.NoError(t, store.WriteIndexVersionState(batch, "ledger1", canonical, state))
	require.NoError(t, batch.Commit())

	got, ok, err := store.ReadIndexVersionState("ledger1", canonical)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uint32(1), got.CurrentVersion)
	assert.Equal(t, uint32(0), got.PendingVersion)
	assert.Empty(t, got.RewriteProgress)
}

func TestIndexVersionState_Absent(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), discardLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	_, ok, err := store.ReadIndexVersionState("ledger1", "missing")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestIndexVersionState_Delete(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), discardLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	canonical := "acct:metadata:score"
	batch := store.NewBatch()
	require.NoError(t, store.WriteIndexVersionState(batch, "ledger1", canonical, readstore.IndexVersionState{CurrentVersion: 1}))
	require.NoError(t, batch.Commit())

	require.NoError(t, store.DeleteIndexVersionState("ledger1", canonical))

	_, ok, err := store.ReadIndexVersionState("ledger1", canonical)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestReadAllIndexVersionStates(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), discardLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	batch := store.NewBatch()
	require.NoError(t, store.WriteIndexVersionState(batch, "l1", "acct:metadata:score", readstore.IndexVersionState{CurrentVersion: 2}))
	require.NoError(t, store.WriteIndexVersionState(batch, "l1", "tx:metadata:tag", readstore.IndexVersionState{CurrentVersion: 1, PendingVersion: 2, RewriteProgress: []byte("c")}))
	require.NoError(t, store.WriteIndexVersionState(batch, "l2", "acct:metadata:score", readstore.IndexVersionState{CurrentVersion: 5}))
	require.NoError(t, batch.Commit())

	entries, err := store.ReadAllIndexVersionStates()
	require.NoError(t, err)
	require.Len(t, entries, 3)

	got := map[string]readstore.IndexVersionState{}
	for _, e := range entries {
		got[e.LedgerName+"/"+e.CanonicalID] = e.State
	}

	assert.Equal(t, uint32(2), got["l1/acct:metadata:score"].CurrentVersion)
	assert.Equal(t, uint32(2), got["l1/tx:metadata:tag"].PendingVersion)
	assert.Equal(t, []byte("c"), got["l1/tx:metadata:tag"].RewriteProgress)
	assert.Equal(t, uint32(5), got["l2/acct:metadata:score"].CurrentVersion)
}

// TestSnapshotVersionResolver_TornReadIsImpossible pins the contract
// the per-replica versioning rests on: a snapshot taken before an
// atomic version switch (current=v_old → current=v_new) must keep
// resolving the pre-switch version, even after the switch has
// committed to the live DB. Without this, a concurrent rewrite commit
// would let a query that already iterates a pre-switch snapshot
// observe v_new while the snapshot itself only holds the partial
// v_new keyspace it had at snapshot time — silent partial results.
func TestSnapshotVersionResolver_TornReadIsImpossible(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), discardLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	const (
		ledger    = "ledger1"
		canonical = "acct:metadata:score"
	)

	batch := store.NewBatch()
	require.NoError(t, store.WriteIndexVersionState(batch, ledger, canonical,
		readstore.IndexVersionState{CurrentVersion: 1, PendingVersion: 2}))
	require.NoError(t, batch.Commit())

	snap := store.NewSnapshot()

	defer func() { _ = snap.Close() }()

	// Live store sees the post-switch state immediately.
	switchBatch := store.NewBatch()
	require.NoError(t, store.WriteIndexVersionState(switchBatch, ledger, canonical,
		readstore.IndexVersionState{CurrentVersion: 2, PendingVersion: 0}))
	require.NoError(t, switchBatch.Commit())

	// The snapshot, taken before the switch, MUST still see v=1.
	resolveFromSnap := readstore.SnapshotVersionResolver(snap, ledger)
	gotSnap, err := resolveFromSnap(canonical)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), gotSnap,
		"snapshot resolver must hold the pre-switch version — otherwise a query that already iterates a pre-switch keyspace would receive a post-switch version, scanning a half-populated v_new range")

	// Sanity: the live store has of course flipped.
	gotLive, _, err := store.ReadIndexVersionState(ledger, canonical)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), gotLive.CurrentVersion)
}

// TestDeleteLedgerIndexes_WipesIndexVersionState pins the F6 fix:
// IndexVersionState rows are ledger-scoped (`[0xFE][0x04][ledger 64B][canonical]`)
// and MUST be swept when a ledger is dropped, alongside the other
// ledger-scoped keyspaces. Pre-fix the SubInternalIndexVersion
// sub-prefix wasn't in `ledgerScopedPrefixes`, so a same-name
// recreate would resurrect a non-zero CurrentVersion the new ledger
// never wrote — queries would scan an empty v=N keyspace and lie to
// the client.
func TestDeleteLedgerIndexes_WipesIndexVersionState(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), discardLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	// Two ledgers with non-trivial version state; only the first
	// gets dropped.
	batch := store.NewBatch()
	require.NoError(t, store.WriteIndexVersionState(batch, "ledger-doomed", "acct:metadata:score",
		readstore.IndexVersionState{CurrentVersion: 2, PendingVersion: 3}))
	require.NoError(t, store.WriteIndexVersionState(batch, "ledger-doomed", "tx:metadata:tag",
		readstore.IndexVersionState{CurrentVersion: 1}))
	require.NoError(t, store.WriteIndexVersionState(batch, "ledger-keep", "acct:metadata:score",
		readstore.IndexVersionState{CurrentVersion: 5}))
	require.NoError(t, batch.Commit())

	dropBatch := store.NewBatch()
	require.NoError(t, readstore.DeleteLedgerIndexes(dropBatch, "ledger-doomed"))
	require.NoError(t, dropBatch.Commit())

	// Dropped ledger: every IndexVersionState row must be gone.
	for _, canonical := range []string{"acct:metadata:score", "tx:metadata:tag"} {
		_, ok, err := store.ReadIndexVersionState("ledger-doomed", canonical)
		require.NoError(t, err)
		assert.False(t, ok,
			"IndexVersionState[%q] survived ledger drop — a same-name recreate would resurrect this version", canonical)
	}

	// Other ledger: untouched.
	got, ok, err := store.ReadIndexVersionState("ledger-keep", "acct:metadata:score")
	require.NoError(t, err)
	require.True(t, ok, "untouched ledger's IndexVersionState must survive a drop on a different ledger")
	assert.Equal(t, uint32(5), got.CurrentVersion)
}

// TestSnapshotVersionResolver_AbsentReturnsZero confirms the
// "no IndexVersionState entry yet" path resolves to (0, nil) so the
// query layer can translate it into ErrIndexBuilding. Distinguishing
// this from a real Pebble error is the load-bearing point of
// returning err from ReadIndexVersionStateFrom.
func TestSnapshotVersionResolver_AbsentReturnsZero(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), discardLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	snap := store.NewSnapshot()

	defer func() { _ = snap.Close() }()

	got, err := readstore.SnapshotVersionResolver(snap, "ledger1")("acct:metadata:never-built")
	require.NoError(t, err, "absent state must NOT be reported as an error — that's the not-yet-built case the query layer translates into ErrIndexBuilding")
	assert.Equal(t, uint32(0), got)
}

// discardLogger implements logging.Logger silently. The readstore package
// has no other test that needs a logger fixture, so this lives here
// next to its sole consumer.
type discardLogger struct{}

var _ logging.Logger = discardLogger{}

func (discardLogger) Tracef(string, ...any)                        {}
func (discardLogger) Debugf(string, ...any)                        {}
func (discardLogger) Infof(string, ...any)                         {}
func (discardLogger) Errorf(string, ...any)                        {}
func (discardLogger) Trace(...any)                                 {}
func (discardLogger) Debug(...any)                                 {}
func (discardLogger) Info(...any)                                  {}
func (discardLogger) Error(...any)                                 {}
func (l discardLogger) WithFields(map[string]any) logging.Logger   { return l }
func (l discardLogger) WithField(string, any) logging.Logger       { return l }
func (l discardLogger) WithContext(context.Context) logging.Logger { return l }
func (discardLogger) Writer() io.Writer                            { return io.Discard }
func (discardLogger) Enabled(logging.Level) bool                   { return false }
