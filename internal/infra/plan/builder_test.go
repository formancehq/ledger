package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/preload"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const testCacheEpoch uint64 = 1

func TestProposalGuard_ReleaseLoaders(t *testing.T) {
	t.Parallel()

	loaders := preload.NewLoaders()

	// Load a value so we can verify it gets cleaned up
	key := attributes.NewU128(10, 20)

	_, err := loaders.Volumes.LoadOrWait(key, 100, testCacheEpoch, func() (*raftcmdpb.VolumePair, error) {
		return &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(1)}, nil
	})
	require.NoError(t, err)

	// Create a guard with a token tracking that key
	tracker := node.NewIndexTracker(1)
	p := &Builder{loaders: loaders, tracker: tracker}
	tracker.Lock() // simulate AcquireProposalGuard having locked

	guard := &ProposalGuard{
		p:     p,
		token: &preload.CleanupToken{Tracked: []preload.TrackedLoader{{Loader: loaders.Volumes, Keys: []attributes.U128{key}}}},
	}

	// Release loaders
	guard.ReleaseLoaders()

	// Token should be nil (idempotent)
	assert.Nil(t, guard.token)

	// The key should have been released from the loader
	loadCount := 0
	_, err = loaders.Volumes.LoadOrWait(key, 100, testCacheEpoch, func() (*raftcmdpb.VolumePair, error) {
		loadCount++

		return &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(2)}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, loadCount, "Key should reload after ReleaseLoaders")

	// Calling again should be safe (idempotent)
	guard.ReleaseLoaders()

	// Release the proposal lock
	guard.Release()
}

func TestProposalGuard_ReleaseAll(t *testing.T) {
	t.Parallel()

	loaders := preload.NewLoaders()

	key := attributes.NewU128(30, 40)

	_, err := loaders.References.LoadOrWait(key, 100, testCacheEpoch, func() (*commonpb.TransactionReferenceValue, error) {
		return &commonpb.TransactionReferenceValue{TransactionId: 1}, nil
	})
	require.NoError(t, err)

	tracker := node.NewIndexTracker(1)
	p := &Builder{loaders: loaders, tracker: tracker}
	tracker.Lock()

	guard := &ProposalGuard{
		p:     p,
		token: &preload.CleanupToken{Tracked: []preload.TrackedLoader{{Loader: loaders.References, Keys: []attributes.U128{key}}}},
	}

	// ReleaseAll should release both the lock and the loaders
	guard.ReleaseAll()

	// Verify loaders were cleaned
	assert.Nil(t, guard.token)

	loadCount := 0
	_, err = loaders.References.LoadOrWait(key, 100, testCacheEpoch, func() (*commonpb.TransactionReferenceValue, error) {
		loadCount++

		return &commonpb.TransactionReferenceValue{TransactionId: 2}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, loadCount, "Key should reload after ReleaseAll")
}

func TestPreloadBuild_ReleaseLoaders(t *testing.T) {
	t.Parallel()

	loaders := preload.NewLoaders()

	key := attributes.NewU128(50, 60)

	_, err := loaders.References.LoadOrWait(key, 100, testCacheEpoch, func() (*commonpb.TransactionReferenceValue, error) {
		return &commonpb.TransactionReferenceValue{TransactionId: 1}, nil
	})
	require.NoError(t, err)

	build := &BuildResult{
		token: &preload.CleanupToken{Tracked: []preload.TrackedLoader{{Loader: loaders.References, Keys: []attributes.U128{key}}}},
	}

	build.ReleaseLoaders()

	// Token should be nil
	assert.Nil(t, build.token)

	// Calling again should be safe (idempotent)
	build.ReleaseLoaders()

	// Key should have been released
	loadCount := 0
	_, err = loaders.References.LoadOrWait(key, 100, testCacheEpoch, func() (*commonpb.TransactionReferenceValue, error) {
		loadCount++

		return &commonpb.TransactionReferenceValue{TransactionId: 2}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, loadCount, "Key should reload after ReleaseLoaders")
}

func TestPreloadBuild_ReleaseLoaders_NilToken(t *testing.T) {
	t.Parallel()

	build := &BuildResult{token: nil}

	// Should not panic with nil token
	build.ReleaseLoaders()
}

func TestPreloader_Loaders(t *testing.T) {
	t.Parallel()

	p := &Builder{loaders: preload.NewLoaders()}

	loaders := p.Loaders()
	assert.NotNil(t, loaders)
	assert.NotNil(t, loaders.Volumes)
	assert.NotNil(t, loaders.Ledgers)
	assert.NotNil(t, loaders.Boundaries)
	assert.NotNil(t, loaders.References)
	assert.NotNil(t, loaders.SinkConfigs)
	assert.NotNil(t, loaders.AccountMetadata)
	assert.NotNil(t, loaders.NumscriptVersions)
	assert.NotNil(t, loaders.Transactions)

	// Verify it returns the same instance
	assert.Same(t, p.loaders, loaders)
}

// TestResolveLedgerID verifies the Builder.ResolveLedgerID resolution path:
// bloom miss, Pebble fallback, and cache hit.
func TestResolveLedgerID(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	attrs := attributes.New()
	c, err := cache.New(1000, meter)
	require.NoError(t, err)

	// Write a LedgerInfo{Name: "test", Id: 42} to Pebble via the Ledger attribute.
	ledgerInfo := &commonpb.LedgerInfo{Name: "test", Id: 42}
	canonical := domain.LedgerKey{Name: "test"}.Bytes()

	batch := store.OpenWriteSession()
	_, err = attrs.Ledger.Set(batch, canonical, ledgerInfo)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	tracker := node.NewIndexTracker(1)

	p := NewBuilder(tracker, c, attrs, store, nil, logger, 0)

	// 1. Bloom miss: resolve a name that does not exist.
	id, ok := p.ResolveLedgerID("nonexistent")
	require.False(t, ok)
	require.Equal(t, uint32(0), id)

	// 2. Pebble fallback: resolve "test" (cache is empty, falls through to Pebble).
	id, ok = p.ResolveLedgerID("test")
	require.True(t, ok)
	require.Equal(t, uint32(42), id)

	// 3. Populate the cache with the LedgerInfo entry and verify cache hit.
	attrID, _ := attributes.MakeKey(canonical)
	c.Ledgers.Put(attrID, attributes.Entry[*commonpb.LedgerInfo]{Data: ledgerInfo})

	id, ok = p.ResolveLedgerID("test")
	require.True(t, ok)
	require.Equal(t, uint32(42), id)
}

// TestResolveLedgerID_BloomNotReadyFallsThrough pins the fix for #318.
// Bloom filters start empty at process start and are populated
// asynchronously (restoreBloomFilters / StartAsyncBloomPopulate). During
// that window IsReady() is false and the raw filter's MayContain returns
// false for every input. ResolveLedgerID used to consult the raw filter
// instead of the IsReady-guarded helper, so it returned (0, false) for
// every pre-existing ledger and admission then rejected the matching
// proposals (ErrBalanceNotPreloaded / ErrLedgerNotFound) until the
// bloom rebuild finished.
func TestResolveLedgerID_BloomNotReadyFallsThrough(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	attrs := attributes.New()
	c, err := cache.New(1000, meter)
	require.NoError(t, err)

	// Persist a ledger to Pebble only; nothing in the cache, nothing in
	// the bloom yet — the exact state during a fresh boot.
	ledgerInfo := &commonpb.LedgerInfo{Name: "test", Id: 42}
	canonical := domain.LedgerKey{Name: "test"}.Bytes()

	batch := store.OpenWriteSession()
	_, err = attrs.Ledger.Set(batch, canonical, ledgerInfo)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Build a bloom FilterSet with the ledgers type enabled. Do NOT mark
	// it ready: that's the exact window the bug used to break.
	bfs := bloom.NewFilterSet(&commonpb.ClusterConfig{
		BloomLedgers: &commonpb.BloomTypeConfig{ExpectedKeys: 1024, FpRate: 0.01},
	}, meter)
	require.NotNil(t, bfs)
	require.False(t, bfs.IsReady(), "precondition: bloom must be in the populating window")

	tracker := node.NewIndexTracker(1)
	p := NewBuilder(tracker, c, attrs, store, bfs, logger, 0)

	// Pre-fix the call short-circuited on the empty bloom and returned
	// (0, false). Post-fix the IsReady guard skips the bloom, the lookup
	// falls through to Pebble, and finds the ledger.
	id, ok := p.ResolveLedgerID("test")
	require.True(t, ok,
		"ResolveLedgerID must fall through to Pebble while the bloom is still populating (#318)")
	require.Equal(t, uint32(42), id)
}

// TestBuildPreloads_DeclaresAbsentNonZeroKey pins the coverage-gap fix
// reported on #451: a proposer that requests a key for a kind without
// zero-value semantics (e.g. transaction references, prepared queries) and
// finds it absent from both bloom and Pebble previously emitted nothing in
// the ExecutionPlan. With the strict Plan the FSM-side read would
// crash the node on the missing declaration, breaking common create paths
// (new transaction reference, new prepared query). Post-fix the resolve
// loop emits a Declare-intent AttributePlan so the View admits the read
// and the underlying KeyStore returns ErrNotFound for legitimate absence.
func TestBuildPreloads_DeclaresAbsentNonZeroKey(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	attrs := attributes.New()
	c, err := cache.New(1000, meter)
	require.NoError(t, err)

	tracker := node.NewIndexTracker(1)
	p := NewBuilder(tracker, c, attrs, store, nil, logger, 0)

	refKey := domain.TransactionReferenceKey{LedgerName: "test", Reference: "fresh-ref"}
	needs := NewNeeds()
	needs.Add(dal.SubAttrReference, refKey.Bytes())

	build, err := p.Build([]WriteOperation{{Needs: needs}})
	require.NoError(t, err)
	defer build.ReleaseLoaders()

	ps := build.ExecutionPlan
	require.NotNil(t, ps)

	// Pebble has nothing for this key and no zero-value preload exists for
	// references — the resolver must emit a Declare-intent AttributePlan to
	// keep the key covered. Without this the FSM Plan would crash
	// the node on read.
	require.Len(t, ps.GetAttributes(), 1,
		"absent non-zero-valued key must still be declared so the FSM-side View admits the read")

	plan := ps.GetAttributes()[0]
	_, isDeclare := plan.GetIntent().(*raftcmdpb.AttributePlan_Declare)
	require.True(t, isDeclare, "intent must be Declare for an absent reference")
	require.Equal(t, uint32(dal.SubAttrReference), plan.GetAttrCode())

	expectedID, _ := attributes.MakeKey(refKey.Bytes())
	require.Equal(t, expectedID[:], plan.GetId().GetId())
}

// TestBuildPreloads_RejectsCacheHorizonExceeded covers the admission-level
// guard against ≥2 cache rotations between propose and apply. With a low
// rotation threshold (10) and the tracker pinned past two boundaries, the
// resolver must short-circuit and surface ErrCacheHorizonExceeded so the
// proposal never reaches Raft (and the audit log records nothing — it is a
// system-level rejection, not a business outcome).
func TestBuildPreloads_RejectsCacheHorizonExceeded(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	attrs := attributes.New()
	// Threshold 10: every 10 indices crosses a generation. Tracker starts
	// past the second boundary so Gen(nextIndex) - currentGeneration >= 2.
	c, err := cache.New(10, meter)
	require.NoError(t, err)
	c.SetCurrentGeneration(0)

	// Tracker at index 25 → Gen(25, 10) = 2, two rotations ahead of
	// currentGeneration (0) → CheckCache returns CacheUnreachable.
	tracker := node.NewIndexTracker(25)
	p := NewBuilder(tracker, c, attrs, store, nil, logger, 0)

	refKey := domain.TransactionReferenceKey{LedgerName: "test", Reference: "ref"}
	needs := NewNeeds()
	needs.Add(dal.SubAttrReference, refKey.Bytes())

	build, buildErr := p.Build([]WriteOperation{{Needs: needs}})
	defer build.ReleaseLoaders()

	require.Error(t, buildErr, "admission must reject when 2+ rotations are predicted")
	require.ErrorIs(t, buildErr, ErrCacheHorizonExceeded,
		"reject must surface ErrCacheHorizonExceeded so the gRPC adapter maps to codes.Unavailable")
}

// TestBuildPreloads_EmitsDeclareOnCacheGuaranteed pins the CacheGuaranteed
// branch: when admission's CheckCache verdict says the key is in Gen0 and
// still will be at apply, the resolver emits Declare (coverage-only, no
// Pebble read).
func TestBuildPreloads_EmitsDeclareOnCacheGuaranteed(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	attrs := attributes.New()
	c, err := cache.New(1000, meter)
	require.NoError(t, err)

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: "alice"},
		Key:        "label",
	}
	id, _ := attributes.MakeKey(metaKey.Bytes())
	c.AccountMetadata.Put(id, attributes.Entry[*commonpb.MetadataValue]{
		Data: &commonpb.MetadataValue{},
	})

	tracker := node.NewIndexTracker(1)
	p := NewBuilder(tracker, c, attrs, store, nil, logger, 0)

	needs := NewNeeds()
	needs.Add(dal.SubAttrMetadata, metaKey.Bytes())

	build, err := p.Build([]WriteOperation{{Needs: needs}})
	require.NoError(t, err)
	defer build.ReleaseLoaders()

	require.Len(t, build.ExecutionPlan.GetAttributes(), 1)
	plan := build.ExecutionPlan.GetAttributes()[0]

	_, isDeclare := plan.GetIntent().(*raftcmdpb.AttributePlan_Declare)
	require.True(t, isDeclare, "CacheGuaranteed must emit Declare — cache already has the value, no seed needed")
	require.Equal(t, uint32(dal.SubAttrMetadata), plan.GetAttrCode())
	require.Equal(t, id[:], plan.GetId().GetId())
}

// TestBuildPreloads_EmitsDeclareOnMissingKey confirms the CacheMiss +
// Pebble-absent branch: nothing to seed, so the resolver emits Declare
// (coverage-only).
func TestBuildPreloads_EmitsDeclareOnMissingKey(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	attrs := attributes.New()
	c, err := cache.New(1000, meter)
	require.NoError(t, err)

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: "alice"},
		Key:        "label",
	}

	tracker := node.NewIndexTracker(1)
	p := NewBuilder(tracker, c, attrs, store, nil, logger, 0)

	needs := NewNeeds()
	needs.Add(dal.SubAttrMetadata, metaKey.Bytes())

	build, err := p.Build([]WriteOperation{{Needs: needs}})
	require.NoError(t, err)
	defer build.ReleaseLoaders()

	require.Len(t, build.ExecutionPlan.GetAttributes(), 1)
	plan := build.ExecutionPlan.GetAttributes()[0]

	_, isDeclare := plan.GetIntent().(*raftcmdpb.AttributePlan_Declare)
	require.True(t, isDeclare, "CacheMiss + Pebble-absent must emit Declare (coverage-only)")
}

// TestBuildPreloads_EmitsDeclareOnBloomShortcut confirms the
// bloom-shortcut path: when admission's bloom filter says "definitely
// not", the resolver skips the Pebble read and emits Declare directly.
func TestBuildPreloads_EmitsDeclareOnBloomShortcut(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	attrs := attributes.New()
	c, err := cache.New(1000, meter)
	require.NoError(t, err)

	bfs := bloom.NewFilterSet(&commonpb.ClusterConfig{
		BloomMetadata: &commonpb.BloomTypeConfig{ExpectedKeys: 1024, FpRate: 0.001},
	}, meter)
	require.NotNil(t, bfs)
	bfs.SetReady(true)
	require.True(t, bfs.IsReady())

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: "alice"},
		Key:        "label",
	}

	tracker := node.NewIndexTracker(1)
	p := NewBuilder(tracker, c, attrs, store, bfs, logger, 0)

	needs := NewNeeds()
	needs.Add(dal.SubAttrMetadata, metaKey.Bytes())

	build, err := p.Build([]WriteOperation{{Needs: needs}})
	require.NoError(t, err)
	defer build.ReleaseLoaders()

	require.Len(t, build.ExecutionPlan.GetAttributes(), 1)
	plan := build.ExecutionPlan.GetAttributes()[0]

	_, isDeclare := plan.GetIntent().(*raftcmdpb.AttributePlan_Declare)
	require.True(t, isDeclare, "bloom-shortcut must emit Declare — the fast path bypasses Pebble entirely")
}
