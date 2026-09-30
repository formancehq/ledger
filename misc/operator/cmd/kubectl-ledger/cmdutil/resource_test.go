package cmdutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

// TestClusterPodName verifies pod names route through the resource prefix,
// matching the operator's "ledger-next-<cr>-<ordinal>" StatefulSet pod naming (EN-1319).
func TestClusterPodName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		crName   string
		ordinal  int
		expected string
	}{
		{name: "ordinal 0", crName: "foo", ordinal: 0, expected: "ledger-next-foo-0"},
		{name: "ordinal 2", crName: "foo", ordinal: 2, expected: "ledger-next-foo-2"},
		{name: "name with dashes", crName: "my-cluster", ordinal: 1, expected: "ledger-next-my-cluster-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.expected, ClusterPodName(tt.crName, tt.ordinal))
		})
	}
}

// The two API groups may use the same Cluster name and namespace. CLI lists
// must only select next workloads, including PVCs and Services.
func TestClusterListsExcludeSameNamedPebbleWorkloads(t *testing.T) {
	t.Parallel()
	var objects []runtime.Object
	for _, engine := range []string{"ledger", "ledger-next"} {
		metadata := metav1.ObjectMeta{
			Namespace: "shared", Name: engine + "-example",
			Labels: map[string]string{LabelName: engine, LabelInstance: "example"},
		}
		objects = append(objects,
			&corev1.Pod{ObjectMeta: metadata},
			&corev1.PersistentVolumeClaim{ObjectMeta: metadata},
			&corev1.Service{ObjectMeta: metadata},
		)
	}
	cs := fake.NewSimpleClientset(objects...)
	pods, err := ClusterPods(context.Background(), cs, "shared", "example")
	require.NoError(t, err)
	require.Len(t, pods.Items, 1)
	require.Equal(t, "ledger-next-example", pods.Items[0].Name)
	pvcs, err := ClusterPVCs(context.Background(), cs, "shared", "example")
	require.NoError(t, err)
	require.Len(t, pvcs.Items, 1)
	require.Equal(t, "ledger-next-example", pvcs.Items[0].Name)
	services, err := Clusters(context.Background(), cs, "shared", "example")
	require.NoError(t, err)
	require.Len(t, services.Items, 1)
	require.Equal(t, "ledger-next-example", services.Items[0].Name)
}
