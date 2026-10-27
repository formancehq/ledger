package bootstrap

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

type fakeAdmission struct {
	mu         sync.Mutex
	calls      int
	err        error
	lastPolicy *commonpb.ClusterPolicy
}

func (f *fakeAdmission) Admit(_ context.Context, _ *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++

	return nil, f.err
}

func (f *fakeAdmission) AdmitClusterPolicy(_ context.Context, policy *commonpb.ClusterPolicy) (*domain.ApplyResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastPolicy = policy.CloneVT()

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

	t.Run("no re-propose on same revision with a different payload", func(t *testing.T) {
		t.Parallel()

		store := newReconcilerTestStore(t)
		cfg := validBaseConfig()

		batch := store.OpenWriteSession()
		require.NoError(t, state.SaveClusterPolicy(batch, &commonpb.ClusterPolicy{
			Revision:             cfg.ClusterPolicyRevision,
			QueryCheckpointLimit: cfg.QueryCheckpointLimit + 1,
		}))
		require.NoError(t, batch.Commit())

		admission := &fakeAdmission{}
		reconcileClusterPolicy(context.Background(), admission, store, cfg, func() bool { return true }, logging.Testing())

		require.Zero(t, admission.callCount(), "a same-revision/different-payload conflict must not be re-proposed")
	})

	t.Run("no propose when a newer revision is already applied", func(t *testing.T) {
		t.Parallel()

		store := newReconcilerTestStore(t)
		cfg := validBaseConfig()

		batch := store.OpenWriteSession()
		require.NoError(t, state.SaveClusterPolicy(batch, &commonpb.ClusterPolicy{
			Revision:                    cfg.ClusterPolicyRevision + 1,
			IdempotencyTtlMicros:        uint64(cfg.IdempotencyTTL.Microseconds()),
			QueryCheckpointLimit:        cfg.QueryCheckpointLimit,
			MetadataMaxEntriesPerEntity: cfg.MetadataMaxEntriesPerEntity,
			MetadataMaxKeyBytes:         cfg.MetadataMaxKeyBytes,
			MetadataMaxValueBytes:       cfg.MetadataMaxValueBytes,
			MetadataMaxEntityBytes:      cfg.MetadataMaxEntityBytes,
			MetadataMaxCommandBytes:     cfg.MetadataMaxCommandBytes,
		}))
		require.NoError(t, batch.Commit())

		admission := &fakeAdmission{}
		reconcileClusterPolicy(context.Background(), admission, store, cfg, func() bool { return true }, logging.Testing())

		require.Zero(t, admission.callCount(), "a newer applied revision must not be overwritten")
	})

	t.Run("does not manufacture a revision when a member reader is present", func(t *testing.T) {
		t.Parallel()

		store := newReconcilerTestStore(t)
		cfg := validBaseConfig()
		batch := store.OpenWriteSession()
		require.NoError(t, state.SaveClusterPolicy(batch, &commonpb.ClusterPolicy{
			Revision:                    cfg.ClusterPolicyRevision + 1,
			IdempotencyTtlMicros:        uint64(cfg.IdempotencyTTL.Microseconds()),
			QueryCheckpointLimit:        cfg.QueryCheckpointLimit,
			MetadataMaxEntriesPerEntity: cfg.MetadataMaxEntriesPerEntity,
			MetadataMaxKeyBytes:         cfg.MetadataMaxKeyBytes,
			MetadataMaxValueBytes:       cfg.MetadataMaxValueBytes,
			MetadataMaxEntityBytes:      cfg.MetadataMaxEntityBytes,
			MetadataMaxCommandBytes:     cfg.MetadataMaxCommandBytes,
		}))
		require.NoError(t, batch.Commit())

		admission := &fakeAdmission{}
		var memberReads atomic.Int32
		reconcileClusterPolicy(context.Background(), admission, store, cfg, func() bool { return true }, logging.Testing(), func() []capabilities.Member {
			memberReads.Add(1)
			return []capabilities.Member{{NodeID: 1, Role: capabilities.Voter}}
		})

		require.Zero(t, admission.callCount(), "member evidence must not bypass the stale revision guard")
		require.EqualValues(t, 1, memberReads.Load(), "matching operator configuration must still allow capability reconciliation")
	})

	t.Run("rejects a newer revision with a configuration conflict", func(t *testing.T) {
		t.Parallel()

		store := newReconcilerTestStore(t)
		cfg := validBaseConfig()
		batch := store.OpenWriteSession()
		require.NoError(t, state.SaveClusterPolicy(batch, &commonpb.ClusterPolicy{
			Revision:             cfg.ClusterPolicyRevision + 1,
			QueryCheckpointLimit: cfg.QueryCheckpointLimit + 1,
		}))
		require.NoError(t, batch.Commit())

		admission := &fakeAdmission{}
		var memberReads atomic.Int32
		reconcileClusterPolicy(context.Background(), admission, store, cfg, func() bool { return true }, logging.Testing(), func() []capabilities.Member {
			memberReads.Add(1)
			return []capabilities.Member{{NodeID: 1, Role: capabilities.Voter}}
		})

		require.Zero(t, admission.callCount(), "a newer conflicting configuration must not be re-proposed")
		require.Zero(t, memberReads.Load(), "configuration conflicts must stop before capability reconciliation")
	})

	t.Run("does not replace a same-revision configuration conflict with a capability bump", func(t *testing.T) {
		t.Parallel()

		store := newReconcilerTestStore(t)
		cfg := validBaseConfig()
		batch := store.OpenWriteSession()
		require.NoError(t, state.SaveClusterPolicy(batch, &commonpb.ClusterPolicy{
			Revision:             cfg.ClusterPolicyRevision,
			QueryCheckpointLimit: cfg.QueryCheckpointLimit + 1,
		}))
		require.NoError(t, batch.Commit())

		admission := &fakeAdmission{}
		reconcileClusterPolicy(context.Background(), admission, store, cfg, func() bool { return true }, logging.Testing(), func() []capabilities.Member {
			return []capabilities.Member{{NodeID: 1, Role: capabilities.Voter}}
		})

		require.Zero(t, admission.callCount(), "same-revision configuration conflicts must remain rejected")
	})

	t.Run("preserves committed capability activations on a revision bump", func(t *testing.T) {
		t.Parallel()

		store := newReconcilerTestStore(t)
		cfg := validBaseConfig()
		batch := store.OpenWriteSession()
		require.NoError(t, state.SaveClusterPolicy(batch, &commonpb.ClusterPolicy{
			Revision: cfg.ClusterPolicyRevision - 1,
			CapabilityActivations: []*commonpb.CapabilityActivation{{
				Id: 7, Active: true,
			}},
		}))
		require.NoError(t, batch.Commit())

		admission := &fakeAdmission{}
		reconcileClusterPolicy(context.Background(), admission, store, cfg, func() bool { return true }, logging.Testing())

		require.Equal(t, 1, admission.callCount())
		require.Equal(t, []*commonpb.CapabilityActivation{{Id: 7, Active: true}}, admission.lastPolicy.GetCapabilityActivations())
	})
}
