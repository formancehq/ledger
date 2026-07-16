package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestValidateSelectorImmutability(t *testing.T) {
	t.Parallel()

	const (
		crName    = "my-ledger"
		namespace = "ledger-v3"
	)

	makeService := func(name string, selector map[string]string) *corev1.Service {
		return &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec:       corev1.ServiceSpec{Selector: selector},
		}
	}
	makeSTS := func(selector map[string]string) *appsv1.StatefulSet {
		return &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName(crName), Namespace: namespace},
			Spec:       appsv1.StatefulSetSpec{Selector: &metav1.LabelSelector{MatchLabels: selector}},
		}
	}

	baselineSelector := map[string]string{
		"app.kubernetes.io/name":     "ledger",
		"app.kubernetes.io/instance": crName,
	}

	headlessDisabled := false
	tests := []struct {
		name             string
		additionalLabels map[string]string
		headlessEnabled  *bool
		ingressGrpc      *ledgerv1alpha1.IngressGrpcSpec
		objects          []client.Object
		wantDrift        bool
	}{
		{
			name:    "nothing exists yet (bootstrap)",
			objects: nil,
		},
		{
			name: "all existing selectors match the desired",
			objects: []client.Object{
				makeService(resourceName(crName), baselineSelector),
				makeService(headlessServiceName(crName), baselineSelector),
				makeService(grpcServiceName(crName), baselineSelector),
				makeSTS(baselineSelector),
			},
		},
		{
			name:             "AdditionalLabels diverges from existing Service selector",
			additionalLabels: map[string]string{"app.formance.com/service": "ledger-v3"},
			objects: []client.Object{
				makeService(resourceName(crName), baselineSelector),
			},
			wantDrift: true,
		},
		{
			name:             "AdditionalLabels diverges from existing StatefulSet selector",
			additionalLabels: map[string]string{"app.formance.com/service": "ledger-v3"},
			objects: []client.Object{
				makeSTS(baselineSelector),
			},
			wantDrift: true,
		},
		{
			name:             "override of default key drifts every existing object",
			additionalLabels: map[string]string{"app.kubernetes.io/name": "ledger-v3"},
			objects: []client.Object{
				makeService(resourceName(crName), baselineSelector),
				makeSTS(baselineSelector),
			},
			wantDrift: true,
		},
		{
			// Disabling HeadlessService in the same edit that tweaks
			// additionalLabels must not stall the reconcile: the operator is
			// about to delete the headless Service anyway, so its old
			// selector is irrelevant. Regression guard for the NumaryBot
			// finding on PR #578.
			name:             "disabled HeadlessService is not checked for drift",
			additionalLabels: map[string]string{"app.formance.com/service": "ledger-v3"},
			headlessEnabled:  &headlessDisabled,
			objects: []client.Object{
				makeService(headlessServiceName(crName), baselineSelector),
			},
		},
		{
			// Same story for the gRPC-ingress-backing Service: when
			// ingressGrpc is disabled the Service will be GC'd, so we
			// don't gate the reconcile on its selector.
			name:             "disabled GrpcService is not checked for drift",
			additionalLabels: map[string]string{"app.formance.com/service": "ledger-v3"},
			ingressGrpc:      &ledgerv1alpha1.IngressGrpcSpec{Enabled: false},
			objects: []client.Object{
				makeService(grpcServiceName(crName), baselineSelector),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))
			require.NoError(t, appsv1.AddToScheme(scheme))

			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.objects...).Build()
			r := &ClusterReconciler{Client: c, Scheme: scheme}

			ls := &ledgerv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: namespace},
				Spec: ledgerv1alpha1.ClusterSpec{
					AdditionalLabels: tt.additionalLabels,
					HeadlessService:  ledgerv1alpha1.HeadlessServiceSpec{Enabled: tt.headlessEnabled},
					IngressGrpc:      tt.ingressGrpc,
				},
			}

			err := r.validateSelectorImmutability(context.Background(), ls)
			if tt.wantDrift {
				require.Error(t, err)
				require.True(t, errors.Is(err, errSelectorDrift),
					"drift errors must wrap errSelectorDrift so the reconcile loop can branch on them")

				return
			}
			require.NoError(t, err)
		})
	}
}

// TestPruneDisabledOptionalServices_RunsBeforeDriftGuard locks in that an
// edit that both disables an optional Service and tweaks additionalLabels
// still gets the optional Service deleted, even when drift is detected on
// the still-wanted primary Service. Regression guard for the NumaryBot
// finding on PR #578: the wanted=false skip in validateSelectorImmutability
// was insufficient on its own because drift on the primary Service aborts
// the reconcile before reconcileHeadlessService can run its deletion path.
func TestPruneDisabledOptionalServices_RunsBeforeDriftGuard(t *testing.T) {
	t.Parallel()

	const (
		crName    = "my-ledger"
		namespace = "ledger-v3"
	)

	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, networkingv1.AddToScheme(scheme))

	headlessSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: headlessServiceName(crName), Namespace: namespace},
		Spec: corev1.ServiceSpec{Selector: map[string]string{
			"app.kubernetes.io/name":     "ledger",
			"app.kubernetes.io/instance": crName,
		}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(headlessSvc).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	disabled := false
	ls := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: namespace},
		Spec: ledgerv1alpha1.ClusterSpec{
			HeadlessService: ledgerv1alpha1.HeadlessServiceSpec{Enabled: &disabled},
		},
	}

	require.NoError(t, r.pruneDisabledOptionalServices(context.Background(), ls))

	err := c.Get(context.Background(), client.ObjectKey{
		Name:      headlessServiceName(crName),
		Namespace: namespace,
	}, &corev1.Service{})
	require.True(t, err != nil, "disabled HeadlessService must be deleted by the pre-pass")
}

// TestPruneDisabledOptionalServices_PrunesGrpcIngress locks in that when
// ingressGrpc is disabled the pre-pass deletes BOTH the backing gRPC
// Service AND the gRPC Ingress, so the drift guard cannot leave an
// orphan Ingress pointing at a deleted backend. Regression guard for
// the NumaryBot finding on PR #578.
func TestPruneDisabledOptionalServices_PrunesGrpcIngress(t *testing.T) {
	t.Parallel()

	const (
		crName    = "my-ledger"
		namespace = "ledger-v3"
	)

	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, networkingv1.AddToScheme(scheme))

	grpcSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: grpcServiceName(crName), Namespace: namespace},
	}
	grpcIngress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: grpcIngressName(crName), Namespace: namespace},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(grpcSvc, grpcIngress).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	// IngressGrpc nil (== disabled) — the pre-pass must remove both
	// the gRPC Service AND the gRPC Ingress.
	ls := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: namespace},
	}

	require.NoError(t, r.pruneDisabledOptionalServices(context.Background(), ls))

	svcErr := c.Get(context.Background(), client.ObjectKey{
		Name:      grpcServiceName(crName),
		Namespace: namespace,
	}, &corev1.Service{})
	require.True(t, svcErr != nil, "disabled gRPC Service must be deleted by the pre-pass")

	ingErr := c.Get(context.Background(), client.ObjectKey{
		Name:      grpcIngressName(crName),
		Namespace: namespace,
	}, &networkingv1.Ingress{})
	require.True(t, ingErr != nil, "disabled gRPC Ingress must be deleted by the pre-pass")
}
