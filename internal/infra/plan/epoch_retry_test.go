package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/preload"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// epochChangingResolver resets the cache at the same point a cluster config
// change can invalidate a builder's snapshot: after Build has captured the
// epoch, before the resolver observes its attribute cache.
type epochChangingResolver struct {
	inner            attrResolver
	cache            *cache.Cache
	changesRemaining int
	calls            int
}

func (r *epochChangingResolver) Resolve(
	keys map[attributes.U128]CoverageEntry,
	nextIndex, boundary, cacheEpoch uint64,
	store dal.PebbleGetter,
	logger logging.Logger,
) (*resolveResult, error) {
	r.calls++
	if r.changesRemaining > 0 {
		r.changesRemaining--
		r.cache.ResetWithThreshold(10, 0)
	}

	return r.inner.Resolve(keys, nextIndex, boundary, cacheEpoch, store, logger)
}

func (r *epochChangingResolver) Loader() preload.LoaderOps { return r.inner.Loader() }

func newEpochRetryBuilder(t *testing.T) (*Builder, *epochChangingResolver, *Coverage) {
	t.Helper()
	logger := logging.Testing()
	meter := noop.NewMeterProvider().Meter("test")
	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	attrs := attributes.New()
	c, err := cache.New(10, meter)
	require.NoError(t, err)
	key := domain.TransactionReferenceKey{LedgerName: "epoch", Reference: "retry"}
	batch := store.OpenWriteSession()
	_, err = attrs.References.Set(batch, key.Bytes(), &commonpb.TransactionReferenceValue{TransactionId: 42})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
	needs := NewCoverage()
	needs.Add(dal.SubAttrReference, key.Bytes())
	builder := NewBuilder(node.NewIndexTracker(1), c, attrs, store, nil, logger, 0)
	resolver := &epochChangingResolver{inner: builder.resolvers[dal.SubAttrReference], cache: c}
	builder.resolvers[dal.SubAttrReference] = resolver

	return builder, resolver, needs
}

func TestBuildRetriesChangedEpochAndUsesFreshPlan(t *testing.T) {
	t.Parallel()
	builder, resolver, needs := newEpochRetryBuilder(t)
	resolver.changesRemaining = 1

	built, err := builder.Build(needs, []WriteOperation{{Coverage: needs}})
	require.NoError(t, err)
	defer built.ReleaseLoaders()
	require.Equal(t, 2, resolver.calls)
	require.Equal(t, builder.cache.Epoch(), built.ExecutionPlan.GetCacheEpoch())
	require.Len(t, built.ExecutionPlan.GetAttributes(), 1)
	var got commonpb.TransactionReferenceValue
	require.NoError(t, got.UnmarshalVT(built.ExecutionPlan.GetAttributes()[0].GetValue().GetRawValue()))
	require.Equal(t, uint64(42), got.GetTransactionId())
}

func TestBuildEpochRetryExhaustionIsRetryable(t *testing.T) {
	t.Parallel()
	builder, resolver, needs := newEpochRetryBuilder(t)
	resolver.changesRemaining = 3

	built, err := builder.Build(needs, []WriteOperation{{Coverage: needs}})
	if built != nil {
		built.ReleaseLoaders()
	}
	require.Equal(t, 3, resolver.calls)
	require.ErrorIs(t, err, errCacheEpochChanged)
	require.ErrorIs(t, err, ErrCacheHorizonExceeded, "adapters must classify exhausted epoch retries as retryable")
}

func TestGuardedEpochChangeIsRetryable(t *testing.T) {
	t.Parallel()
	builder, resolver, needs := newEpochRetryBuilder(t)
	built, err := builder.Build(needs, []WriteOperation{{Coverage: needs}})
	require.NoError(t, err)
	defer built.ReleaseLoaders()
	resolver.changesRemaining = 1
	builder.tracker.Advance(11) // cross a generation so the guard must rebuild

	updated, guard, err := builder.AcquireProposalGuard(built)
	if guard != nil {
		defer guard.ReleaseAll()
	}
	require.Nil(t, updated)
	require.Equal(t, 2, resolver.calls)
	require.ErrorIs(t, err, errCacheEpochChanged)
	require.ErrorIs(t, err, ErrCacheHorizonExceeded)
}
