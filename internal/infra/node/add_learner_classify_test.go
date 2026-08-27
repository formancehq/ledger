package node

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddLearnerPathsRejectInvalidInstanceIDBeforeRaftAccess(t *testing.T) {
	t.Parallel()

	n := &Node{}
	require.ErrorContains(t, n.AddLearner(context.Background(), 1, "r:1", "s:1", nil), "instance_id must be 16 bytes")
	require.ErrorContains(t, n.JoinAsLearner(context.Background(), 1, "r:1", "s:1", []byte("short")), "instance_id must be 16 bytes")
}

// TestClassifyExistingLearner locks in the EN-1436 fail-fast decision.
//
// The stale-progress fail-fast is scoped to the explicit JoinAsLearner boot
// path. On that path a non-zero Progress.Match must fail fast REGARDLESS of whether the stored instance_id
// matches the incoming one: the fresh-identity (WAL-wiped) rejoin is the case
// that previously slipped through as a benign ConfChangeUpdateNode refresh and
// re-introduced the "tocommit out of range" crash loop.
//
// The admin AddLearner API also requires a 16-byte instance_id. An active
// member with the same identity is an idempotent retry; a different identity
// is rejected because it cannot safely inherit already-replicated progress.
func TestClassifyExistingLearner(t *testing.T) {
	t.Parallel()

	old := []byte("0123456789abcdef")   // 16-byte stored identity
	fresh := []byte("fedcba9876543210") // 16-byte incoming identity (post-wipe)

	t.Run("stale progress, matching identity", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, existingLearnerStaleProgress,
			classifyExistingLearner(5, old, old, true),
			"Match>0 on the boot path must fail fast even when the identity is unchanged")
	})

	t.Run("stale progress, fresh identity after WAL wipe", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, existingLearnerStaleProgress,
			classifyExistingLearner(5, old, fresh, true),
			"Match>0 must fail fast on the fresh-identity rejoin — the bug the finding flagged")
	})

	t.Run("admin retry, active member, matching identity is idempotent", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, existingLearnerAlreadyInCluster,
			classifyExistingLearner(5, old, old, false))
	})

	t.Run("admin retry, active member, different identity is stale", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, existingLearnerStaleProgress,
			classifyExistingLearner(5, old, fresh, false))
	})

	t.Run("no replicated state, matching identity is idempotent", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, existingLearnerAlreadyInCluster,
			classifyExistingLearner(0, old, old, true))
	})

	t.Run("no replicated state, differing identity needs refresh", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, existingLearnerNeedsRefresh,
			classifyExistingLearner(0, old, fresh, true),
			"Match==0 with a stale stored identity is a benign UpdateNode refresh")
	})

	t.Run("admin call, no replicated state, differing identity needs refresh", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, existingLearnerNeedsRefresh,
			classifyExistingLearner(0, old, fresh, false))
	})
}
