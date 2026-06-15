package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestUpdateClusterConfig verifies the invariant the method exists to
// protect: HashGenerator is rebuilt iff the algorithm changes; LastClusterConfig
// always becomes the supplied cfg.
func TestUpdateClusterConfig(t *testing.T) {
	t.Parallel()

	t.Run("same algorithm leaves HashGenerator instance untouched", func(t *testing.T) {
		t.Parallel()

		s := NewFSMState("test-cluster")
		originalHG := s.HashGenerator

		cfg := &commonpb.ClusterConfig{HashAlgorithm: commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3}
		s.UpdateClusterConfig(cfg)

		require.Same(t, originalHG, s.HashGenerator, "HashGenerator must be reused when algorithm unchanged")
		require.Same(t, cfg, s.LastClusterConfig)
	})

	t.Run("algorithm change rebuilds HashGenerator", func(t *testing.T) {
		t.Parallel()

		s := NewFSMState("test-cluster")
		originalHG := s.HashGenerator

		cfg := &commonpb.ClusterConfig{HashAlgorithm: commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3}
		s.UpdateClusterConfig(cfg)

		require.NotSame(t, originalHG, s.HashGenerator, "HashGenerator must be rebuilt when algorithm changes")
		require.Equal(t, commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3, s.HashGenerator.Algorithm())
		require.Same(t, cfg, s.LastClusterConfig)
	})

	t.Run("reverting to original algorithm rebuilds again", func(t *testing.T) {
		t.Parallel()

		s := NewFSMState("test-cluster")

		s.UpdateClusterConfig(&commonpb.ClusterConfig{HashAlgorithm: commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3})
		hgAfterFirstSwap := s.HashGenerator

		s.UpdateClusterConfig(&commonpb.ClusterConfig{HashAlgorithm: commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3})

		require.NotSame(t, hgAfterFirstSwap, s.HashGenerator)
		require.Equal(t, commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, s.HashGenerator.Algorithm())
	})
}

// TestAppendAuditEntry verifies the invariant the method exists to protect:
// sequence and hash advance together — the returned sequence is the value
// before the bump, LastAuditHash is set to the supplied hash, and
// NextAuditSequenceID is incremented by exactly one. Multiple calls produce
// monotonic sequences.
func TestAppendAuditEntry(t *testing.T) {
	t.Parallel()

	t.Run("first call returns initial sequence and advances both fields", func(t *testing.T) {
		t.Parallel()

		s := NewFSMState("test-cluster")
		require.Equal(t, uint64(1), s.NextAuditSequenceID)
		require.Empty(t, s.LastAuditHash)

		seq := s.AppendAuditEntry([]byte("hash-1"))

		require.Equal(t, uint64(1), seq, "returned sequence must be the pre-bump value")
		require.Equal(t, []byte("hash-1"), s.LastAuditHash)
		require.Equal(t, uint64(2), s.NextAuditSequenceID)
	})

	t.Run("subsequent calls keep sequence monotonic and overwrite LastAuditHash", func(t *testing.T) {
		t.Parallel()

		s := NewFSMState("test-cluster")

		seq1 := s.AppendAuditEntry([]byte("hash-1"))
		seq2 := s.AppendAuditEntry([]byte("hash-2"))
		seq3 := s.AppendAuditEntry([]byte("hash-3"))

		require.Equal(t, []uint64{1, 2, 3}, []uint64{seq1, seq2, seq3})
		require.Equal(t, []byte("hash-3"), s.LastAuditHash, "LastAuditHash carries the latest hash")
		require.Equal(t, uint64(4), s.NextAuditSequenceID)
	})

	t.Run("empty hash is recorded verbatim (no special casing)", func(t *testing.T) {
		t.Parallel()

		s := NewFSMState("test-cluster")
		s.LastAuditHash = []byte("seed")

		seq := s.AppendAuditEntry(nil)

		require.Equal(t, uint64(1), seq)
		require.Nil(t, s.LastAuditHash, "nil hash overwrites a previous non-nil hash")
		require.Equal(t, uint64(2), s.NextAuditSequenceID)
	})
}
