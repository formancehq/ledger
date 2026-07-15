package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// TestUpdateStatus_RemovesStaleConditions verifies that a condition removed from
// the in-memory ledger during reconcile is actually cleared from the persisted
// status, not merely dropped from the in-memory copy. Regression test for the
// additive-merge bug that left DeletionProtectionInactive (and any other
// conditionally-removed condition) stuck in .status.conditions forever.
func TestUpdateStatus_RemovesStaleConditions(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	// Persisted object already carries a stale DeletionProtectionInactive warning
	// plus an unrelated condition that must survive the update.
	replicas := int32(1)
	persisted := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "default"},
		Spec:       ledgerv1alpha1.ClusterSpec{Replicas: &replicas},
		Status: ledgerv1alpha1.ClusterStatus{
			Conditions: []metav1.Condition{
				{Type: "DeletionProtectionInactive", Status: metav1.ConditionTrue, Reason: "ClusterPolicyNotInstalled"},
				{Type: "ConfigValid", Status: metav1.ConditionTrue, Reason: "Valid"},
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(persisted).
		WithStatusSubresource(&ledgerv1alpha1.Cluster{}).
		Build()

	r := &ClusterReconciler{Client: c, Scheme: scheme}

	// In-memory ledger after a reconcile in which protection became active: the
	// warning was removed from the slice; ConfigValid remains.
	inMemory := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "default"},
		Status: ledgerv1alpha1.ClusterStatus{
			Conditions: []metav1.Condition{
				{Type: "ConfigValid", Status: metav1.ConditionTrue, Reason: "Valid"},
			},
		},
	}

	require.NoError(t, r.updateStatus(context.Background(), inMemory))

	got := &ledgerv1alpha1.Cluster{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "ls", Namespace: "default"}, got))

	assert.Nil(t, meta.FindStatusCondition(got.Status.Conditions, "DeletionProtectionInactive"),
		"stale condition removed in-memory must be cleared from persisted status")
	assert.NotNil(t, meta.FindStatusCondition(got.Status.Conditions, "ConfigValid"),
		"conditions still present in-memory must be retained")
}
