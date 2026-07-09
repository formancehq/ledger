//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestReconcile_StatusPending(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newCluster("pending", ns)
	require.NoError(t, k8sClient.Create(ctx, ls))

	// envtest has no kubelet, so ReadyReplicas will always be 0 → Pending
	updated := &ledgerv1alpha1.Cluster{}
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "pending", Namespace: ns}, updated); err != nil {
			return false
		}
		return updated.Status.Phase == "Pending"
	}, "phase should be Pending")

	assert.Equal(t, int32(0), updated.Status.ReadyReplicas)

	// Ready condition should be False
	cond := meta.FindStatusCondition(updated.Status.Conditions, "Ready")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "ReplicasNotReady", cond.Reason)
}

func TestReconcile_ObservedGeneration(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newCluster("obsgen", ns)
	require.NoError(t, k8sClient.Create(ctx, ls))

	updated := &ledgerv1alpha1.Cluster{}
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "obsgen", Namespace: ns}, updated); err != nil {
			return false
		}
		return updated.Status.ObservedGeneration == updated.Generation
	}, "observedGeneration should match generation")
}
