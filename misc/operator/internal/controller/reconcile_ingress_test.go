//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestReconcile_IngressHTTP(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newLedgerService("ing-http", ns)
	ls.Spec.Ingress = &ledgerv1alpha1.IngressSpec{
		Enabled:   true,
		ClassName: "nginx",
		Hosts: []ledgerv1alpha1.IngressHost{
			{
				Host: "ledger.example.com",
				Paths: []ledgerv1alpha1.IngressPath{
					{Path: "/api", PathType: "Prefix"},
				},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, ls))

	ing := &networkingv1.Ingress{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-ing-http", Namespace: ns}, ing) == nil
	}, "HTTP Ingress should be created")

	require.NotNil(t, ing.Spec.IngressClassName)
	assert.Equal(t, "nginx", *ing.Spec.IngressClassName)
	require.Len(t, ing.Spec.Rules, 1)
	assert.Equal(t, "ledger.example.com", ing.Spec.Rules[0].Host)

	paths := ing.Spec.Rules[0].HTTP.Paths
	require.Len(t, paths, 1)
	assert.Equal(t, "/api", paths[0].Path)
	assert.Equal(t, networkingv1.PathTypePrefix, *paths[0].PathType)

	assert.Equal(t, "ledger-ing-http", paths[0].Backend.Service.Name)
	assert.Equal(t, int32(9000), paths[0].Backend.Service.Port.Number)

	requireOwnerRef(t, ing.OwnerReferences, "ing-http")
}

func TestReconcile_IngressGrpc(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newLedgerService("ing-grpc", ns)
	ls.Spec.IngressGrpc = &ledgerv1alpha1.IngressGrpcSpec{
		Enabled:   true,
		ClassName: "custom-class",
		Hosts: []ledgerv1alpha1.IngressHost{
			{Host: "grpc.example.com"},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, ls))

	ing := &networkingv1.Ingress{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-ing-grpc-grpc", Namespace: ns}, ing) == nil
	}, "gRPC Ingress should be created")

	require.Len(t, ing.Spec.Rules, 1)
	paths := ing.Spec.Rules[0].HTTP.Paths
	require.Len(t, paths, 1)
	assert.Equal(t, "ledger-ing-grpc-grpc", paths[0].Backend.Service.Name)
	assert.Equal(t, int32(8888), paths[0].Backend.Service.Port.Number)
}

func TestReconcile_IngressDisabledCleansUp(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newLedgerService("ing-cleanup", ns)
	ls.Spec.Ingress = &ledgerv1alpha1.IngressSpec{
		Enabled:   true,
		ClassName: "nginx",
		Hosts: []ledgerv1alpha1.IngressHost{
			{Host: "ledger.example.com"},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, ls))

	ing := &networkingv1.Ingress{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-ing-cleanup", Namespace: ns}, ing) == nil
	}, "Ingress should be created")

	updated := &ledgerv1alpha1.LedgerService{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "ing-cleanup", Namespace: ns}, updated))
	updated.Spec.Ingress.Enabled = false
	require.NoError(t, k8sClient.Update(ctx, updated))

	requireEventually(t, func() bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-ing-cleanup", Namespace: ns}, ing)
		return apierrors.IsNotFound(err)
	}, "Ingress should be deleted after disabling")
}

func TestReconcile_IngressDefaultPaths(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newLedgerService("ing-defpath", ns)
	ls.Spec.Ingress = &ledgerv1alpha1.IngressSpec{
		Enabled: true,
		Hosts: []ledgerv1alpha1.IngressHost{
			{Host: "ledger.example.com"},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, ls))

	ing := &networkingv1.Ingress{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-ing-defpath", Namespace: ns}, ing) == nil
	}, "Ingress should be created")

	require.Len(t, ing.Spec.Rules, 1)
	paths := ing.Spec.Rules[0].HTTP.Paths
	require.Len(t, paths, 1)
	assert.Equal(t, "/", paths[0].Path)
	assert.Equal(t, networkingv1.PathTypePrefix, *paths[0].PathType)
}
