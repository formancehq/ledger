package bootstrap

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// The reconciler keeps ticking until stopped, so a transient failure retries.
func TestClusterPolicyReconciler_TicksUntilStopped(t *testing.T) {
	t.Parallel()

	var count atomic.Int32

	r := NewClusterPolicyReconciler(func(context.Context) { count.Add(1) })
	r.interval = 5 * time.Millisecond
	r.Start()

	require.Eventually(t, func() bool { return count.Load() >= 3 }, 2*time.Second, 5*time.Millisecond,
		"reconcile must run repeatedly on the ticker")

	r.Stop()

	// Stop is synchronous (waits for the goroutine), so the count is now final.
	settled := count.Load()
	require.Never(t, func() bool { return count.Load() > settled }, 100*time.Millisecond, 10*time.Millisecond,
		"no reconcile must run after Stop")
}

type fakeAdmission struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (f *fakeAdmission) Admit(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++

	return nil, f.err
}

func (f *fakeAdmission) Barrier(_ context.Context) (uint64, error) { return 0, nil }

func (f *fakeAdmission) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
}

func newReconcilerTestStore(t *testing.T) *dal.Store {
	t.Helper()

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close() })

	return store
}

func TestReconcileClusterPolicy(t *testing.T) {
	t.Parallel()

	t.Run("skips when not leader", func(t *testing.T) {
		t.Parallel()

		admission := &fakeAdmission{}
		reconcileClusterPolicy(context.Background(), admission, newReconcilerTestStore(t), validBaseConfig(),
			func() bool { return false }, logging.Testing())

		require.Zero(t, admission.callCount(), "a non-leader must not propose")
	})

	t.Run("proposes when no policy is committed", func(t *testing.T) {
		t.Parallel()

		admission := &fakeAdmission{}
		reconcileClusterPolicy(context.Background(), admission, newReconcilerTestStore(t), validBaseConfig(),
			func() bool { return true }, logging.Testing())

		require.Equal(t, 1, admission.callCount(), "a leader with no committed policy must propose")
	})

	t.Run("no-op when the desired revision is already applied", func(t *testing.T) {
		t.Parallel()

		store := newReconcilerTestStore(t)
		cfg := validBaseConfig()

		batch := store.OpenWriteSession()
		require.NoError(t, state.SaveClusterPolicy(batch, &commonpb.ClusterPolicy{
			Revision:             cfg.ClusterPolicyRevision,
			IdempotencyTtlMicros: uint64(cfg.IdempotencyTTL.Microseconds()),
			QueryCheckpointLimit: cfg.QueryCheckpointLimit,
		}))
		require.NoError(t, batch.Commit())

		admission := &fakeAdmission{}
		reconcileClusterPolicy(context.Background(), admission, store, cfg, func() bool { return true }, logging.Testing())

		require.Zero(t, admission.callCount(), "an already-applied revision must not be re-proposed")
	})
}
