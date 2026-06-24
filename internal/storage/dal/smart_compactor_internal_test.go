package dal

import (
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func newTestCompactor(t *testing.T, s *Store) *SmartCompactor {
	t.Helper()

	ctx := logging.TestingContext()

	return NewSmartCompactor(s, logging.FromContext(ctx), make(chan struct{}))
}

// TestSmartCompactor_runCompaction_OpenStore exercises the full compaction loop
// synchronously: every prefix compacts successfully and the final metrics log
// runs. runCompaction must reset the compacting flag on return.
func TestSmartCompactor_runCompaction_OpenStore(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	c := newTestCompactor(t, s)

	c.compacting.Store(true)
	c.runCompaction("test", allCompactPrefixes)

	require.False(t, c.compacting.Load())
}

// TestSmartCompactor_runCompaction_ClosedStore covers the ErrStoreClosed abort
// branch: compactRange returns ErrStoreClosed on the first prefix, so the loop
// aborts and the compacting flag is still reset.
func TestSmartCompactor_runCompaction_ClosedStore(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	require.NoError(t, s.Close())

	c := newTestCompactor(t, s)

	c.compacting.Store(true)
	c.runCompaction("test", allCompactPrefixes)

	require.False(t, c.compacting.Load())
}

// TestSmartCompactor_runCompaction_ShutdownAborts covers the stopCh shutdown
// branch: with an already-closed stopCh, the first iteration aborts before any
// compaction runs.
func TestSmartCompactor_runCompaction_ShutdownAborts(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	c := newTestCompactor(t, s)

	stop := make(chan struct{})
	close(stop)
	c.stopCh = stop

	c.compacting.Store(true)
	c.runCompaction("test", allCompactPrefixes)

	require.False(t, c.compacting.Load())
}

// TestSmartCompactor_runCompaction_NonFatalCompactError covers the non-fatal
// error branch: a prefix with start > end makes pebble.Compact return an error
// that is not ErrStoreClosed, so runCompaction logs it and continues.
func TestSmartCompactor_runCompaction_NonFatalCompactError(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	c := newTestCompactor(t, s)

	c.compacting.Store(true)
	c.runCompaction("test", []compactPrefix{{name: "bad-range", start: 0x02, end: 0x01}})

	require.False(t, c.compacting.Load())
}

// TestCompactAll_OpenStore covers CompactAll's success path: every prefix
// compacts through compactRange and the method returns nil.
func TestCompactAll_OpenStore(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	require.NoError(t, s.CompactAll())
}

// TestSmartCompactor_compactPrefixes_ClosedStoreSkips covers the pre-goroutine
// guard: metricsIfOpen reports the store closed, so compactPrefixes logs and
// returns without launching the goroutine, resetting the compacting flag it set.
func TestSmartCompactor_compactPrefixes_ClosedStoreSkips(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	require.NoError(t, s.Close())

	c := newTestCompactor(t, s)

	c.compactPrefixes("test", allCompactPrefixes)

	require.False(t, c.compacting.Load())
}
