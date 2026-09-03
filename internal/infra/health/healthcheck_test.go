package health

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/infra/monitoring/diskusage"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// TestHealthChecker_NonLeaderResetsWriteGate verifies that a node which is not
// the leader clears any write-gate block state on its next check cycle. Without
// this, a node that blocked writes while it was leader (e.g. a volume filled)
// would keep fail-closed and spuriously reject writes after losing leadership,
// for up to one health-check interval. The leader-only verdict is always
// re-derived on the current leader.
func TestHealthChecker_NonLeaderResetsWriteGate(t *testing.T) {
	t.Parallel()

	ns := NewMocknodeState(gomock.NewController(t))
	ns.EXPECT().IsLeader().Return(false).AnyTimes()

	hc := &HealthChecker{node: ns}

	// Simulate stale block state carried over from a prior leadership term.
	hc.gate.Store(&gateState{diskBlocked: true, skewBlocked: true})
	require.Error(t, hc.CheckWritesAllowed())

	// The non-leader branch returns before touching the collector / service
	// pool (both nil here), so this exercises only the reset.
	hc.check(make(chan struct{}))

	s := hc.gate.Load()
	require.NotNil(t, s)
	require.False(t, s.diskBlocked)
	require.False(t, s.skewBlocked)
	require.NoError(t, hc.CheckWritesAllowed())
}

func TestRemoteVolumeValidity(t *testing.T) {
	t.Parallel()

	valid := &clusterpb.VolumeUsage{
		TotalBytes:   100,
		ObservedAtUs: uint64(time.Now().UnixMicro()),
		SampleAgeMs:  uint64(diskusage.MaximumSampleAge.Milliseconds()),
		Valid:        true,
	}
	ok, diagnostic := remoteVolumeValidity(valid)
	require.True(t, ok)
	require.Empty(t, diagnostic)

	tests := []struct {
		name       string
		volume     *clusterpb.VolumeUsage
		diagnostic string
	}{
		{name: "missing volume", diagnostic: "missing"},
		{name: "failed collection", volume: &clusterpb.VolumeUsage{Error: "input/output error"}, diagnostic: "input/output error"},
		{name: "zero total", volume: &clusterpb.VolumeUsage{Valid: true, ObservedAtUs: 1}, diagnostic: "totalBytes"},
		{name: "missing observation time", volume: &clusterpb.VolumeUsage{Valid: true, TotalBytes: 100}, diagnostic: "observedAtUs"},
		{
			name: "stale sample",
			volume: &clusterpb.VolumeUsage{
				Valid:        true,
				TotalBytes:   100,
				ObservedAtUs: 1,
				SampleAgeMs:  uint64(diskusage.MaximumSampleAge.Milliseconds()) + 1,
			},
			diagnostic: "stale",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ok, diagnostic := remoteVolumeValidity(tt.volume)
			require.False(t, ok)
			require.Contains(t, diagnostic, tt.diagnostic)
		})
	}
}

// TestCheckWritesAllowed_NoTornStateBetweenReasons locks in the single-atomic
// publish: while the gate flips between two distinct block reasons (disk-only
// and skew-only), a concurrent reader must never observe an "allowed" state. The
// previous two-atomic design (separate diskBlocked/skewBlocked stores) had a
// torn-read window where both bits briefly read false mid-transition; this test
// fails under that design and passes once the verdict is published atomically.
// Run under -race to also catch the data race on the old fields.
func TestCheckWritesAllowed_NoTornStateBetweenReasons(t *testing.T) {
	t.Parallel()

	hc := &HealthChecker{}
	hc.gate.Store(&gateState{diskBlocked: true})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 200000 {
			if i%2 == 0 {
				hc.gate.Store(&gateState{diskBlocked: true, skewBlocked: false})
			} else {
				hc.gate.Store(&gateState{diskBlocked: false, skewBlocked: true})
			}
		}
	}()

	for {
		select {
		case <-done:
			return
		default:
			// Either reason blocks; the gate must never appear open mid-transition.
			require.Error(t, hc.CheckWritesAllowed())
		}
	}
}
