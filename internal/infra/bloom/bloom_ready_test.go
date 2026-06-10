package bloom

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	commonpb "github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// addKey populates the current snapshot's Volume filter with the given key
// bytes. Mirrors what the bloom populator does during the not-ready window
// (Pebble scan + Filter.Add), but keeps the test free of Pebble plumbing.
func addKey(t *testing.T, fs *FilterSet, key []byte) {
	t.Helper()

	f := fs.Snapshot().FilterForAttrType(dal.SubAttrVolume)
	if f == nil {
		t.Fatalf("Volume filter is nil — bloom config mismatch")
	}

	f.Add(attributes.HashU128(attributes.DefaultSeeds, key))
}

func bloomCfg() *commonpb.ClusterConfig {
	return &commonpb.ClusterConfig{
		BloomVolumes: &commonpb.BloomTypeConfig{ExpectedKeys: 1000, FpRate: 0.01},
	}
}

// TestFilterSet_SnapshotPairsReadyAndFilters is the contract for #317: a
// captured snapshot must keep its (filters, ready) pair stable across a
// concurrent Rebuild on the live FilterSet. The whole point of moving
// readiness into the snapshot is to give callers a single atomic load
// that resolves both fields consistently.
func TestFilterSet_SnapshotPairsReadyAndFilters(t *testing.T) {
	t.Parallel()

	meter := noop.NewMeterProvider().Meter("test")
	fs := NewFilterSet(bloomCfg(), meter)
	require.NotNil(t, fs)

	keyBytes := []byte("ledger-1")
	id := attributes.HashU128(attributes.DefaultSeeds, keyBytes)

	addKey(t, fs, keyBytes)
	fs.SetReady(true)

	// Capture the populated, ready snapshot, then trigger a Rebuild on the
	// live FilterSet. The captured snapshot must remain unchanged.
	captured := fs.Snapshot()
	require.True(t, captured.Ready(), "captured snapshot should be ready")
	require.NotNil(t, captured.FilterForAttrType(dal.SubAttrVolume))
	require.True(t, captured.FilterForAttrType(dal.SubAttrVolume).MayContain(id))

	fs.Rebuild(bloomCfg())

	// Live FilterSet now exposes a fresh, not-ready, empty snapshot.
	live := fs.Snapshot()
	require.False(t, live.Ready(), "fresh post-Rebuild snapshot must not be ready")
	require.False(t, live.FilterForAttrType(dal.SubAttrVolume).MayContain(id),
		"fresh post-Rebuild filter must not yet contain the key")

	// But the previously captured snapshot is unaffected — its ready bit
	// and filter pointers are immutable per-snap.
	require.True(t, captured.Ready(), "captured snapshot must not flip back to not-ready")
	require.True(t, captured.FilterForAttrType(dal.SubAttrVolume).MayContain(id),
		"captured snapshot must continue to report the key as present")
}

// TestFilterSet_SetReadyIfEpoch_RebuildInvalidates is the staleness guard:
// a background populator that captured the epoch before a Rebuild must not
// be able to mark the post-Rebuild snapshot as ready.
func TestFilterSet_SetReadyIfEpoch_RebuildInvalidates(t *testing.T) {
	t.Parallel()

	meter := noop.NewMeterProvider().Meter("test")
	fs := NewFilterSet(bloomCfg(), meter)
	require.NotNil(t, fs)

	epoch := fs.Epoch()
	fs.Rebuild(bloomCfg()) // bumps epoch under the lock

	require.False(t, fs.SetReadyIfEpoch(epoch),
		"stale populator must not be allowed to promote the post-Rebuild snapshot")
	require.False(t, fs.Snapshot().Ready())

	// Same call with the fresh epoch succeeds.
	require.True(t, fs.SetReadyIfEpoch(fs.Epoch()))
	require.True(t, fs.Snapshot().Ready())
}

// TestFilterSet_NoReadyWithEmptyFilterUnderContention is the #317 regression:
// under repeated Rebuild + populate + SetReadyIfEpoch cycles, a reader that
// observes Ready()=true must always see the populator's writes in the same
// snapshot. The pre-fix code separated readiness (FilterSet.ready
// atomic.Bool) from the filters (filterSnapshot pointer); a reader could
// read ready=true from an earlier revision and then load a freshly-swapped
// empty snapshot, treating present keys as absent.
//
// The test is deliberately tight: 200ms of pressure with goroutine yield
// hints is enough to surface the race on the previous code (verified by
// reverting the bloom.go changes locally), while keeping CI under a second.
func TestFilterSet_NoReadyWithEmptyFilterUnderContention(t *testing.T) {
	t.Parallel()

	meter := noop.NewMeterProvider().Meter("test")
	fs := NewFilterSet(bloomCfg(), meter)
	require.NotNil(t, fs)

	keyBytes := []byte("ledger-canary")
	id := attributes.HashU128(attributes.DefaultSeeds, keyBytes)

	// Initial state: populated + ready.
	addKey(t, fs, keyBytes)
	fs.SetReady(true)

	var (
		stop          atomic.Bool
		violations    atomic.Int64
		writerCycles  atomic.Int64
		readerLookups atomic.Int64
	)

	var wg sync.WaitGroup

	const readers = 8
	for range readers {
		wg.Go(func() {
			for !stop.Load() {
				snap := fs.Snapshot()
				if !snap.Ready() {
					continue
				}
				f := snap.FilterForAttrType(dal.SubAttrVolume)
				if f == nil || !f.MayContain(id) {
					violations.Add(1)
				}
				readerLookups.Add(1)
			}
		})
	}

	// Writer: cycle Rebuild → populate → SetReadyIfEpoch. The populator
	// writes BEFORE flipping to ready, so any reader that observes
	// ready=true must see the key.

	wg.Go(func() {
		for !stop.Load() {
			fs.Rebuild(bloomCfg())
			epoch := fs.Epoch()
			addKey(t, fs, keyBytes)

			if !fs.SetReadyIfEpoch(epoch) {
				continue
			}
			writerCycles.Add(1)
		}
	})

	time.Sleep(200 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	require.Positive(t, readerLookups.Load(), "readers should have made progress")
	require.Positive(t, writerCycles.Load(), "writer should have completed at least one cycle")
	require.Zero(t, violations.Load(),
		"reader observed Ready=true but MayContain(key)=false in %d/%d lookups (#317 regression)",
		violations.Load(), readerLookups.Load())
}
