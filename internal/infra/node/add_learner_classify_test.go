package node

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestClassifyExistingLearner locks in the EN-1436 fail-fast decision: when the
// leader already tracks the joining nodeID, a non-zero Progress.Match must fail
// fast with stale-progress REGARDLESS of whether the stored instance_id matches
// the incoming one. The fresh-identity (WAL-wiped) rejoin is the case that
// previously slipped through as a benign ConfChangeUpdateNode refresh and
// re-introduced the "tocommit out of range" crash loop.
func TestClassifyExistingLearner(t *testing.T) {
	t.Parallel()

	old := []byte("0123456789abcdef")   // 16-byte stored identity
	fresh := []byte("fedcba9876543210") // 16-byte incoming identity (post-wipe)

	t.Run("stale progress, matching identity", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, existingLearnerStaleProgress,
			classifyExistingLearner(5, old, true, old),
			"Match>0 must fail fast even when the identity is unchanged")
	})

	t.Run("stale progress, fresh identity after WAL wipe", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, existingLearnerStaleProgress,
			classifyExistingLearner(5, old, true, fresh),
			"Match>0 must fail fast on the fresh-identity rejoin — the bug the finding flagged")
	})

	t.Run("stale progress, empty stored identity", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, existingLearnerStaleProgress,
			classifyExistingLearner(5, nil, false, fresh),
			"Match>0 outranks the missing-row refresh path")
	})

	t.Run("no replicated state, matching identity is idempotent", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, existingLearnerAlreadyInCluster,
			classifyExistingLearner(0, old, true, old))
	})

	t.Run("no replicated state, differing identity needs refresh", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, existingLearnerNeedsRefresh,
			classifyExistingLearner(0, old, true, fresh),
			"Match==0 with a stale stored identity is a benign UpdateNode refresh")
	})

	t.Run("no replicated state, empty stored identity needs refresh", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, existingLearnerNeedsRefresh,
			classifyExistingLearner(0, nil, true, fresh),
			"admin AddLearner + boot: stored is empty, fill it in via UpdateNode")
	})

	t.Run("no replicated state, no row is idempotent", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, existingLearnerAlreadyInCluster,
			classifyExistingLearner(0, nil, false, fresh),
			"no peer row to refresh and nothing replicated — treat as idempotent")
	})

	t.Run("no replicated state, non-16-byte incoming id is idempotent", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, existingLearnerAlreadyInCluster,
			classifyExistingLearner(0, old, true, []byte("short")),
			"an admin AddLearner without a booted pod carries no 16-byte id; do not refresh")
	})
}
