package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestJoinAuthError_Message pins the actionable wording of the fatal
// cluster-join authentication error (EN-1080). The message must tell the
// operator exactly which lever to pull, distinguishing the missing-secret
// case (the joining node has no --cluster-secret) from the mismatched-secret
// case (it has one, but the target rejected it).
func TestJoinAuthError_Message(t *testing.T) {
	t.Parallel()

	t.Run("missing secret", func(t *testing.T) {
		t.Parallel()

		err := &JoinAuthError{
			PeerID:      2,
			PeerAddress: "node-1:7777",
			HasSecret:   false,
			Detail:      "missing authorization metadata on Raft RPC",
		}

		msg := err.Error()
		require.Contains(t, msg, "peer 2 (node-1:7777)")
		require.Contains(t, msg, "inter-node authentication failed")
		require.Contains(t, msg, "missing authorization metadata on Raft RPC")
		// Actionable hint for the missing-secret case.
		require.Contains(t, msg, "without --cluster-secret")
		require.Contains(t, msg, "set --cluster-secret")
	})

	t.Run("peer discovery phase (no peer id yet)", func(t *testing.T) {
		t.Parallel()

		// During Phase 1 peer discovery the joining node only knows the
		// --join address, not the target's node id, so PeerID is 0 and the
		// message must fall back to the raw address without a "peer 0 (...)"
		// prefix.
		err := &JoinAuthError{
			PeerAddress: "node-1:7777",
			HasSecret:   false,
			Detail:      "missing authorization metadata on Raft RPC",
		}

		msg := err.Error()
		require.Contains(t, msg, "rejected by node-1:7777")
		require.NotContains(t, msg, "peer 0")
		require.Contains(t, msg, "set --cluster-secret")
	})

	t.Run("mismatched secret", func(t *testing.T) {
		t.Parallel()

		err := &JoinAuthError{
			PeerID:      3,
			PeerAddress: "node-1:7777",
			HasSecret:   true,
			Detail:      "invalid cluster credentials on Raft RPC",
		}

		msg := err.Error()
		require.Contains(t, msg, "peer 3 (node-1:7777)")
		require.Contains(t, msg, "invalid cluster credentials on Raft RPC")
		// Actionable hint for the mismatched-secret case.
		require.Contains(t, msg, "started with --cluster-secret")
		require.Contains(t, msg, "verify the secret matches")
	})
}
