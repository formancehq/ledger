//go:build integration

package controller

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// updateClusterSpec applies mutate to a fresh copy of the Cluster, retrying
// only on resourceVersion conflicts (the reconciler concurrently persists the
// generated clusterID), and returns the admission result.
func updateClusterSpec(t *testing.T, key types.NamespacedName, mutate func(*ledgerv1alpha1.ClusterSpec)) error {
	t.Helper()

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cluster := &ledgerv1alpha1.Cluster{}
		if err := k8sClient.Get(ctx, key, cluster); err != nil {
			return err
		}
		mutate(&cluster.Spec)

		return k8sClient.Update(ctx, cluster)
	})
}

// TestClusterResourceNameCRDValidation exercises the generated CRD schema and
// CEL rules for spec.resourceName against a real API server: DNS-1035 pattern,
// 52-char cap, immutability, and the create-time-only rule that rejects adding
// or removing the field after creation.
func TestClusterResourceNameCRDValidation(t *testing.T) {
	ns := createTestNamespace(t)

	t.Run("rejects invalid values at creation", func(t *testing.T) {
		for i, value := range []string{"Payments", "-payments", "payments-", "9payments", "pay.ments", strings.Repeat("a", 53)} {
			cluster := newCluster(fmt.Sprintf("invalid-%d", i), ns)
			cluster.Spec.ResourceName = value
			err := k8sClient.Create(ctx, cluster)
			require.Error(t, err, "resourceName %q must be rejected", value)
			require.True(t, apierrors.IsInvalid(err), "resourceName %q: %v", value, err)
		}
	})

	t.Run("accepts a 52-char DNS-1035 label and reconciles under it", func(t *testing.T) {
		cluster := newCluster("max-length", ns)
		cluster.Spec.ResourceName = strings.Repeat("a", 52)
		require.NoError(t, k8sClient.Create(ctx, cluster))
		requireEventually(t, func() bool {
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: cluster.Spec.ResourceName}, &appsv1.StatefulSet{}) == nil
		}, "StatefulSet should be created under the resourceName")
	})

	t.Run("is immutable once set", func(t *testing.T) {
		cluster := newCluster("immutable", ns)
		cluster.Spec.ResourceName = "immutable-rn"
		require.NoError(t, k8sClient.Create(ctx, cluster))
		key := types.NamespacedName{Namespace: ns, Name: cluster.Name}

		err := updateClusterSpec(t, key, func(spec *ledgerv1alpha1.ClusterSpec) { spec.ResourceName = "changed-rn" })
		require.Error(t, err)
		require.Contains(t, err.Error(), "resourceName is immutable")

		// Unrelated edits keep working while resourceName is unchanged.
		require.NoError(t, updateClusterSpec(t, key, func(spec *ledgerv1alpha1.ClusterSpec) { spec.Replicas = new(int32(5)) }))
	})

	t.Run("cannot be removed after creation", func(t *testing.T) {
		cluster := newCluster("remove", ns)
		cluster.Spec.ResourceName = "remove-rn"
		require.NoError(t, k8sClient.Create(ctx, cluster))

		err := updateClusterSpec(t, types.NamespacedName{Namespace: ns, Name: cluster.Name}, func(spec *ledgerv1alpha1.ClusterSpec) { spec.ResourceName = "" })
		require.Error(t, err)
		require.Contains(t, err.Error(), "resourceName can only be set at creation")
	})

	t.Run("cannot be added after creation", func(t *testing.T) {
		cluster := newCluster("add", ns)
		require.NoError(t, k8sClient.Create(ctx, cluster))

		err := updateClusterSpec(t, types.NamespacedName{Namespace: ns, Name: cluster.Name}, func(spec *ledgerv1alpha1.ClusterSpec) { spec.ResourceName = "added-rn" })
		require.Error(t, err)
		require.Contains(t, err.Error(), "resourceName can only be set at creation")
	})
}
