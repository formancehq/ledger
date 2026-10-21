package cmdutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"

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

// ownershipFixture is Cluster "billing" with spec.resourceName "payments", as
// read from the API server (with its UID), the StatefulSet "payments" it
// controls and that StatefulSet's pod "payments-0".
func ownershipFixture() (*ledgerv1alpha1.Cluster, *appsv1.StatefulSet, *corev1.Pod) {
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "billing", Namespace: "ns", UID: "billing-uid"},
		Spec:       ledgerv1alpha1.ClusterSpec{ResourceName: "payments"},
	}
	selector := map[string]string{LabelName: LabelValue, LabelInstance: "billing"}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "payments", Namespace: "ns", UID: "sts-uid",
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cluster, ledgerv1alpha1.GroupVersion.WithKind("Cluster"))},
		},
		Spec: appsv1.StatefulSetSpec{Selector: &metav1.LabelSelector{MatchLabels: selector}},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "payments-0", Namespace: "ns", UID: "pod-uid", Labels: selector,
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(sts, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))},
	}}

	return cluster, sts, pod
}

// TestClusterPodRequiresOwnershipChain proves the plugin reaches a pod only
// through the chain Cluster UID -> StatefulSet "<base>" -> pod: a pod is
// refused when the StatefulSet under the shared base name is controlled by
// another Cluster (a second Cluster with the same spec.resourceName), or when
// the pod under "<base>-<ordinal>" is controlled by another StatefulSet UID (a
// deleted Cluster's runtime awaiting garbage collection).
func TestClusterPodRequiresOwnershipChain(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	other := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "ns", UID: "orders-uid"}}

	t.Run("owned chain", func(t *testing.T) {
		t.Parallel()
		cluster, sts, pod := ownershipFixture()
		cs := k8sfake.NewClientset(sts, pod)

		got, err := ClusterPod(ctx, cs, cluster, 0)
		require.NoError(t, err)
		require.Equal(t, types.UID("pod-uid"), got.UID)
		pods, err := ClusterPods(ctx, cs, cluster)
		require.NoError(t, err)
		require.Len(t, pods, 1)
		require.Equal(t, "payments-0", pods[0].Name)
	})

	t.Run("StatefulSet controlled by another Cluster", func(t *testing.T) {
		t.Parallel()
		cluster, sts, pod := ownershipFixture()
		sts.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(other, ledgerv1alpha1.GroupVersion.WithKind("Cluster"))}
		cs := k8sfake.NewClientset(sts, pod)

		_, err := ClusterPod(ctx, cs, cluster, 0)
		require.ErrorIs(t, err, ErrNotOwned)
		require.EqualError(t, err, `not controlled by the Cluster: StatefulSet ns/payments is controlled by Cluster/orders (uid orders-uid), not by Cluster "billing" (uid billing-uid); `+
			`its base name "payments" conflicts with another owner (see the Cluster's ResourceNamesAvailable condition), refusing to access its pods`)
		pods, err := ClusterPods(ctx, cs, cluster)
		require.ErrorIs(t, err, ErrNotOwned)
		require.Empty(t, pods)
	})

	t.Run("pod controlled by a replaced StatefulSet", func(t *testing.T) {
		t.Parallel()
		cluster, sts, pod := ownershipFixture()
		oldStatefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "ns", UID: "old-sts-uid"}}
		pod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(oldStatefulSet, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))}
		cs := k8sfake.NewClientset(sts, pod)

		_, err := ClusterPod(ctx, cs, cluster, 0)
		require.ErrorIs(t, err, ErrNotOwned)
		require.EqualError(t, err, `not controlled by the Cluster: pod ns/payments-0 is controlled by StatefulSet/payments (uid old-sts-uid), `+
			`not by StatefulSet payments (uid sts-uid) of Cluster "billing"; refusing to access it`)
		pods, err := ClusterPods(ctx, cs, cluster)
		require.NoError(t, err)
		require.Empty(t, pods, "a pod matching the selector but controlled by another StatefulSet must not be listed")
	})

	t.Run("missing StatefulSet", func(t *testing.T) {
		t.Parallel()
		cluster, _, pod := ownershipFixture()

		_, err := ClusterPod(ctx, k8sfake.NewClientset(pod), cluster, 0)
		require.True(t, apierrors.IsNotFound(err), "got %v", err)
		require.NotErrorIs(t, err, ErrNotOwned)
	})
}

// TestClusterPVCsSelectsByDerivedName proves the plugin finds a Cluster's
// claims by their derived names "<wal|data>-<base>-<ordinal>" whatever their
// labels (spec.additionalLabels can override app.kubernetes.io/instance), and
// ignores a claim labelled for the Cluster under another name.
func TestClusterPVCsSelectsByDerivedName(t *testing.T) {
	t.Parallel()
	cluster, _, _ := ownershipFixture()
	instanceLabel := map[string]string{LabelName: LabelValue, LabelInstance: "billing"}
	claim := func(name string, labels map[string]string) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: labels}}
	}
	cs := k8sfake.NewClientset(
		claim("data-payments-0", map[string]string{LabelName: LabelValue, LabelInstance: "orders"}), // instance overridden
		claim("wal-payments-0", nil),
		claim("data-payments-4", instanceLabel), // kept from a removed replica
		claim("data-payments-x-1", instanceLabel),
		claim("data-payments-01", instanceLabel),
		claim("cache-payments-0", instanceLabel),
		claim("data-ledger-billing-0", instanceLabel),
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-payments-1", Namespace: "other-ns"}},
	)

	list, err := ClusterPVCs(context.Background(), cs, cluster)

	require.NoError(t, err)
	var names []string
	for _, pvc := range list.Items {
		names = append(names, pvc.Name)
	}
	require.ElementsMatch(t, []string{"data-payments-0", "wal-payments-0", "data-payments-4"}, names)
}
