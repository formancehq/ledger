package preload

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func TestProposalGuard_ReleaseLoaders(t *testing.T) {
	t.Parallel()

	loaders := NewLoaders()

	// Load a value so we can verify it gets cleaned up
	key := attributes.NewU128(10, 20)

	_, err := loaders.Volumes.LoadOrWait(key, 100, func() (*raftcmdpb.VolumePair, uint64, error) {
		return &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(1)}, 0, nil
	})
	require.NoError(t, err)

	// Create a guard with a token tracking that key
	p := &Preloader{loaders: loaders}
	p.proposeMu.Lock() // simulate AcquireProposalGuard having locked

	guard := &ProposalGuard{
		p:     p,
		token: &CleanupToken{Volumes: []attributes.U128{key}},
	}

	// Release loaders
	guard.ReleaseLoaders()

	// Token should be nil (idempotent)
	assert.Nil(t, guard.token)

	// The key should have been released from the loader
	loadCount := 0
	_, err = loaders.Volumes.LoadOrWait(key, 100, func() (*raftcmdpb.VolumePair, uint64, error) {
		loadCount++

		return &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(2)}, 0, nil
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

	_, err := loaders.IdempotencyKeys.LoadOrWait(key, 100, func() (*commonpb.IdempotencyKeyValue, uint64, error) {
		return &commonpb.IdempotencyKeyValue{LogSequence: 1, Hash: []byte("h")}, 0, nil
	})
	require.NoError(t, err)

	p := &Preloader{loaders: loaders}
	p.proposeMu.Lock()

	guard := &ProposalGuard{
		p:     p,
		token: &CleanupToken{IdempotencyKeys: []attributes.U128{key}},
	}

	// ReleaseAll should release both the lock and the loaders
	guard.ReleaseAll()

	// Verify the mutex was unlocked by trying to lock it again
	p.proposeMu.Lock()
	p.proposeMu.Unlock() //nolint:staticcheck // SA2001: empty critical section is intentional

	// Verify loaders were cleaned
	assert.Nil(t, guard.token)

	loadCount := 0
	_, err = loaders.IdempotencyKeys.LoadOrWait(key, 100, func() (*commonpb.IdempotencyKeyValue, uint64, error) {
		loadCount++

		return &commonpb.IdempotencyKeyValue{LogSequence: 2, Hash: []byte("h2")}, 0, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, loadCount, "Key should reload after ReleaseAll")
}

func TestPreloadBuild_ReleaseLoaders(t *testing.T) {
	t.Parallel()

	loaders := NewLoaders()

	key := attributes.NewU128(50, 60)

	_, err := loaders.References.LoadOrWait(key, 100, func() (*commonpb.TransactionReferenceValue, uint64, error) {
		return &commonpb.TransactionReferenceValue{TransactionId: 1}, 0, nil
	})
	require.NoError(t, err)

	build := &PreloadBuild{
		token: &CleanupToken{References: []attributes.U128{key}},
	}

	build.ReleaseLoaders(loaders)

	// Token should be nil
	assert.Nil(t, build.token)

	// Calling again should be safe (idempotent)
	build.ReleaseLoaders(loaders)

	// Key should have been released
	loadCount := 0
	_, err = loaders.References.LoadOrWait(key, 100, func() (*commonpb.TransactionReferenceValue, uint64, error) {
		loadCount++

		return &commonpb.TransactionReferenceValue{TransactionId: 2}, 0, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, loadCount, "Key should reload after ReleaseLoaders")
}

func TestPreloadBuild_ReleaseLoaders_NilToken(t *testing.T) {
	t.Parallel()

	loaders := NewLoaders()
	build := &PreloadBuild{token: nil}

	// Should not panic with nil token
	build.ReleaseLoaders(loaders)
}

func TestPreloader_Loaders(t *testing.T) {
	t.Parallel()

	p := &Preloader{loaders: NewLoaders()}

	loaders := p.Loaders()
	assert.NotNil(t, loaders)
	assert.NotNil(t, loaders.Volumes)
	assert.NotNil(t, loaders.Ledgers)
	assert.NotNil(t, loaders.Boundaries)
	assert.NotNil(t, loaders.IdempotencyKeys)
	assert.NotNil(t, loaders.References)
	assert.NotNil(t, loaders.SinkConfigs)
	assert.NotNil(t, loaders.AccountMetadata)
	assert.NotNil(t, loaders.NumscriptVersions)
	assert.NotNil(t, loaders.NumscriptEntries)
	assert.NotNil(t, loaders.NumscriptParsed)
	assert.NotNil(t, loaders.Transactions)

	// Verify it returns the same instance
	assert.Same(t, p.loaders, loaders)
}
