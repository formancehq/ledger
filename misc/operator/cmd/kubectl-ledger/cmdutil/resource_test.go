package cmdutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestClusterPodName verifies pod names route through the resource prefix,
// matching the operator's "ledger-<cr>-<ordinal>" StatefulSet pod naming (EN-1319).
func TestClusterPodName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		crName   string
		ordinal  int
		expected string
	}{
		{name: "ordinal 0", crName: "foo", ordinal: 0, expected: "ledger-foo-0"},
		{name: "ordinal 2", crName: "foo", ordinal: 2, expected: "ledger-foo-2"},
		{name: "name with dashes", crName: "my-cluster", ordinal: 1, expected: "ledger-my-cluster-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.expected, ClusterPodName(tt.crName, tt.ordinal))
		})
	}
}
