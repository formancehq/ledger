package node

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A refused promotion must return a typed sentinel so the gRPC adapter can
// report the reason instead of sanitizing it to an unknown server error.
func TestPromoteLearner_RefusalsAreTyped(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		nodeID uint64
		want   error
	}{
		{name: "non-member", nodeID: 987654, want: ErrNodeNotInCluster},
		{name: "already voter", nodeID: 1, want: ErrNodeAlreadyVoter},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n := newConfiguredPeersTestNode(t)
			results := make(chan error, 1)
			go func() {
				results <- n.PromoteLearner(t.Context(), tc.nodeID)
			}()
			cmd := <-n.clusterCommandCh
			err := cmd.fn()
			require.ErrorIs(t, err, tc.want)
			cmd.errCh <- err
			require.ErrorIs(t, <-results, tc.want)
		})
	}
}
