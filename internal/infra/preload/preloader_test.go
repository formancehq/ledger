package preload

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestProposalGuard_ReleaseLoaders(t *testing.T) {
	t.Parallel()

	loaders := NewLoaders()

	// Load a value so we can verify it gets cleaned up
	key := attributes.NewU128(10, 20)

	_, err := loaders.Volumes.LoadOrWait(key, 100, func() (*raftcmdpb.VolumePair, error) {
		return &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(1)}, nil
	})
	require.NoError(t, err)

	// Create a guard with a token tracking that key
	tracker := node.NewIndexTracker(1)
	p := &Preloader{loaders: loaders, tracker: tracker}
	tracker.Lock() // simulate AcquireProposalGuard having locked

	guard := &ProposalGuard{
		p:     p,
		token: &CleanupToken{tracked: []trackedLoader{{loader: loaders.Volumes, keys: []attributes.U128{key}}}},
	}

	// Release loaders
	guard.ReleaseLoaders()

	// Token should be nil (idempotent)
	assert.Nil(t, guard.token)

	// The key should have been released from the loader
	loadCount := 0
	_, err = loaders.Volumes.LoadOrWait(key, 100, func() (*raftcmdpb.VolumePair, error) {
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

	loaders := NewLoaders()

	key := attributes.NewU128(30, 40)

	_, err := loaders.References.LoadOrWait(key, 100, func() (*commonpb.TransactionReferenceValue, error) {
		return &commonpb.TransactionReferenceValue{TransactionId: 1}, nil
	})
	require.NoError(t, err)

	tracker := node.NewIndexTracker(1)
	p := &Preloader{loaders: loaders, tracker: tracker}
	tracker.Lock()

	guard := &ProposalGuard{
		p:     p,
		token: &CleanupToken{tracked: []trackedLoader{{loader: loaders.References, keys: []attributes.U128{key}}}},
	}

	// ReleaseAll should release both the lock and the loaders
	guard.ReleaseAll()

	// Verify loaders were cleaned
	assert.Nil(t, guard.token)

	loadCount := 0
	_, err = loaders.References.LoadOrWait(key, 100, func() (*commonpb.TransactionReferenceValue, error) {
		loadCount++

		return &commonpb.TransactionReferenceValue{TransactionId: 2}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, loadCount, "Key should reload after ReleaseAll")
}

func TestPreloadBuild_ReleaseLoaders(t *testing.T) {
	t.Parallel()

	loaders := NewLoaders()

	key := attributes.NewU128(50, 60)

	_, err := loaders.References.LoadOrWait(key, 100, func() (*commonpb.TransactionReferenceValue, error) {
		return &commonpb.TransactionReferenceValue{TransactionId: 1}, nil
	})
	require.NoError(t, err)

	build := &PreloadBuild{
		token: &CleanupToken{tracked: []trackedLoader{{loader: loaders.References, keys: []attributes.U128{key}}}},
	}

	build.ReleaseLoaders()

	// Token should be nil
	assert.Nil(t, build.token)

	// Calling again should be safe (idempotent)
	build.ReleaseLoaders()

	// Key should have been released
	loadCount := 0
	_, err = loaders.References.LoadOrWait(key, 100, func() (*commonpb.TransactionReferenceValue, error) {
		loadCount++

		return &commonpb.TransactionReferenceValue{TransactionId: 2}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, loadCount, "Key should reload after ReleaseLoaders")
}

func TestPreloadBuild_ReleaseLoaders_NilToken(t *testing.T) {
	t.Parallel()

	build := &PreloadBuild{token: nil}

	// Should not panic with nil token
	build.ReleaseLoaders()
}

func TestPreloader_Loaders(t *testing.T) {
	t.Parallel()

	p := &Preloader{loaders: NewLoaders()}

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

// TestResolveLedgerID verifies the Preloader.ResolveLedgerID resolution path:
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

	batch := store.NewBatch()
	_, err = attrs.Ledger.Set(batch, canonical, ledgerInfo)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	tracker := node.NewIndexTracker(1)

	p := New(tracker, c, attrs, store, nil, logger)

	// 1. Bloom miss: resolve a name that does not exist.
	id, ok := p.ResolveLedgerID("nonexistent")
	require.False(t, ok)
	require.Equal(t, uint32(0), id)

	// 2. Pebble fallback: resolve "test" (cache is empty, falls through to Pebble).
	id, ok = p.ResolveLedgerID("test")
	require.True(t, ok)
	require.Equal(t, uint32(42), id)

	// 3. Populate the cache with the LedgerInfo entry and verify cache hit.
	attrID, _ := attributes.MakeKey(attributes.DefaultSeeds, canonical)
	c.Ledgers.Put(attrID, attributes.Entry[*commonpb.LedgerInfo]{Data: ledgerInfo})

	id, ok = p.ResolveLedgerID("test")
	require.True(t, ok)
	require.Equal(t, uint32(42), id)
}
