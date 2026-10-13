package cmdutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// TestClusterPodName verifies pod names route through the Cluster base name:
// the operator's "ledger-<cr>-<ordinal>" StatefulSet pod naming (EN-1319), or
// "<resourceName>-<ordinal>" when spec.resourceName overrides the base name.
func TestClusterPodName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		crName       string
		resourceName string
		ordinal      int
		expected     string
	}{
		{name: "ordinal 0", crName: "foo", ordinal: 0, expected: "ledger-foo-0"},
		{name: "ordinal 2", crName: "foo", ordinal: 2, expected: "ledger-foo-2"},
		{name: "name with dashes", crName: "my-cluster", ordinal: 1, expected: "ledger-my-cluster-1"},
		{name: "resourceName override", crName: "jdxmvkvwlyiy-cukg", resourceName: "payments", ordinal: 1, expected: "payments-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cluster := &ledgerv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: tt.crName},
				Spec:       ledgerv1alpha1.ClusterSpec{ResourceName: tt.resourceName},
			}
			require.Equal(t, tt.expected, ClusterPodName(cluster, tt.ordinal))
		})
	}
}
