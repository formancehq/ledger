package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

// TestUpdateStatus_PreservesErrorPhaseAcrossStatefulSetState locks in that
// once reconcile has parked the CR in Phase=Error (validateSpec failure,
// selector drift), the deferred status update does not overwrite the phase
// with Running/Degraded/Pending derived from StatefulSet readiness.
// Regression guard for the NumaryBot finding on PR #578: hiding the Error
// phase behind a healthy StatefulSet makes users / automation miss the
// blocked state.
func TestUpdateStatus_PreservesErrorPhaseAcrossStatefulSetState(t *testing.T) {
	t.Parallel()

	const (
		crName    = "my-ledger"
		namespace = "ledger-v3"
	)

	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	replicas := int32(3)

	tests := []struct {
		name      string
		stsReady  int32
		stsExists bool
		wantPhase string
	}{
		{name: "no StatefulSet (bootstrap)", stsExists: false, wantPhase: "Error"},
		{name: "StatefulSet fully ready", stsExists: true, stsReady: replicas, wantPhase: "Error"},
		{name: "StatefulSet partially ready", stsExists: true, stsReady: 1, wantPhase: "Error"},
		{name: "StatefulSet zero ready", stsExists: true, stsReady: 0, wantPhase: "Error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ls := &ledgerv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: namespace},
				Spec:       ledgerv1alpha1.ClusterSpec{Replicas: &replicas},
			}
			objects := []runtime.Object{ls}
			if tt.stsExists {
				objects = append(objects, &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{Name: resourceName(crName), Namespace: namespace},
					Status:     appsv1.StatefulSetStatus{ReadyReplicas: tt.stsReady},
				})
			}

			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithStatusSubresource(&ledgerv1alpha1.Cluster{}).
				WithRuntimeObjects(objects...).
				Build()
			r := &ClusterReconciler{Client: c, Scheme: scheme}

			// In-memory ledger carrying the Error phase set during reconcile.
			ls.Status.Phase = "Error"

			require.NoError(t, r.updateStatus(context.Background(), ls))

			latest := &ledgerv1alpha1.Cluster{}
			require.NoError(t, c.Get(context.Background(),
				types.NamespacedName{Name: crName, Namespace: namespace}, latest))
			require.Equal(t, tt.wantPhase, latest.Status.Phase,
				"Error phase must survive recomputation against StatefulSet readiness")
		})
	}
}

// TestUpdateStatus_RecomputesPhaseAfterRecovery is the counterpart of the
// preservation test: once the reconcile loop has cleared the in-memory
// Phase (which Reconcile does at the top after the initial Get), the
// deferred updateStatus must recompute Phase from StatefulSet readiness
// rather than carrying over a previously-persisted Error. Regression
// guard for the NumaryBot finding on PR #578: without the reset at the
// top of Reconcile, a CR that recovered from a drift error would stay
// parked in Phase=Error forever.
func TestUpdateStatus_RecomputesPhaseAfterRecovery(t *testing.T) {
	t.Parallel()

	const (
		crName    = "my-ledger"
		namespace = "ledger-v3"
	)

	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	replicas := int32(3)

	// Persisted CR still carries the previous Phase=Error in its status.
	// Reconcile will reset the in-memory Phase before calling updateStatus.
	persisted := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: namespace},
		Spec:       ledgerv1alpha1.ClusterSpec{Replicas: &replicas},
		Status:     ledgerv1alpha1.ClusterStatus{Phase: "Error"},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName(crName), Namespace: namespace},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: replicas},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&ledgerv1alpha1.Cluster{}).
		WithRuntimeObjects(persisted, sts).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	// Model the Reconcile reset: in-memory Phase is empty by the time we
	// reach updateStatus. The previous Error lives only in the persisted
	// status now.
	inMem := persisted.DeepCopy()
	inMem.Status.Phase = ""

	require.NoError(t, r.updateStatus(context.Background(), inMem))

	latest := &ledgerv1alpha1.Cluster{}
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Name: crName, Namespace: namespace}, latest))
	require.Equal(t, "Running", latest.Status.Phase,
		"recovery must surface as Running, not the stale persisted Error")
}
