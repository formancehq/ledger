//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestReconcile_NetworkPolicyEnabled(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newCluster("np-basic", ns)
	ls.Spec.NetworkPolicy = &ledgerv1alpha1.NetworkPolicySpec{
		Enabled: true,
	}
	require.NoError(t, k8sClient.Create(ctx, ls))

	np := &networkingv1.NetworkPolicy{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-np-basic", Namespace: ns}, np) == nil
	}, "NetworkPolicy should be created")

	// Verify common labels.
	assert.Equal(t, "ledger-operator", np.Labels[labelManagedBy])
	assert.Equal(t, "ledger", np.Labels[labelName])
	assert.Equal(t, "np-basic", np.Labels[labelInstance])

	// Verify selector labels.
	require.NotNil(t, np.Spec.PodSelector.MatchLabels)
	assert.Equal(t, "ledger", np.Spec.PodSelector.MatchLabels[labelName])
	assert.Equal(t, "np-basic", np.Spec.PodSelector.MatchLabels[labelInstance])

	// Verify policy types.
	require.Len(t, np.Spec.PolicyTypes, 1)
	assert.Equal(t, networkingv1.PolicyTypeEgress, np.Spec.PolicyTypes[0])

	// Verify 3 egress rules.
	require.Len(t, np.Spec.Egress, 3)

	// Rule 0: inter-node.
	interNode := np.Spec.Egress[0]
	require.Len(t, interNode.To, 1)
	require.NotNil(t, interNode.To[0].PodSelector)
	assert.Equal(t, "np-basic", interNode.To[0].PodSelector.MatchLabels[labelInstance])
	require.Len(t, interNode.Ports, 3)
	assert.Equal(t, int32(7777), interNode.Ports[0].Port.IntVal)
	assert.Equal(t, int32(8888), interNode.Ports[1].Port.IntVal)
	assert.Equal(t, int32(9000), interNode.Ports[2].Port.IntVal)
	tcp := corev1.ProtocolTCP
	assert.Equal(t, &tcp, interNode.Ports[0].Protocol)

	// Rule 1: DNS.
	dns := np.Spec.Egress[1]
	require.Len(t, dns.To, 1)
	require.NotNil(t, dns.To[0].NamespaceSelector)
	require.NotNil(t, dns.To[0].PodSelector)
	assert.Equal(t, "kube-dns", dns.To[0].PodSelector.MatchLabels["k8s-app"])
	require.Len(t, dns.Ports, 2)
	assert.Equal(t, int32(53), dns.Ports[0].Port.IntVal)
	udp := corev1.ProtocolUDP
	assert.Equal(t, &udp, dns.Ports[0].Protocol)
	assert.Equal(t, &tcp, dns.Ports[1].Protocol)

	// Rule 2: external.
	ext := np.Spec.Egress[2]
	require.Len(t, ext.To, 1)
	require.NotNil(t, ext.To[0].IPBlock)
	assert.Equal(t, "0.0.0.0/0", ext.To[0].IPBlock.CIDR)
	assert.Equal(t, []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}, ext.To[0].IPBlock.Except)

	requireOwnerRef(t, np.OwnerReferences, "np-basic")
}

func TestReconcile_NetworkPolicyCustomCIDR(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newCluster("np-cidr", ns)
	ls.Spec.NetworkPolicy = &ledgerv1alpha1.NetworkPolicySpec{
		Enabled:            true,
		ExternalCIDRExcept: []string{"10.0.0.0/8", "100.64.0.0/10"},
	}
	require.NoError(t, k8sClient.Create(ctx, ls))

	np := &networkingv1.NetworkPolicy{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-np-cidr", Namespace: ns}, np) == nil
	}, "NetworkPolicy should be created")

	require.Len(t, np.Spec.Egress, 3)
	ext := np.Spec.Egress[2]
	require.NotNil(t, ext.To[0].IPBlock)
	assert.Equal(t, []string{"10.0.0.0/8", "100.64.0.0/10"}, ext.To[0].IPBlock.Except)
}

func TestReconcile_NetworkPolicyDisabledCleansUp(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newCluster("np-cleanup", ns)
	ls.Spec.NetworkPolicy = &ledgerv1alpha1.NetworkPolicySpec{
		Enabled: true,
	}
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Wait for NetworkPolicy creation.
	np := &networkingv1.NetworkPolicy{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-np-cleanup", Namespace: ns}, np) == nil
	}, "NetworkPolicy should be created")

	// Disable NetworkPolicy.
	updated := &ledgerv1alpha1.Cluster{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "np-cleanup", Namespace: ns}, updated))
	updated.Spec.NetworkPolicy.Enabled = false
	require.NoError(t, k8sClient.Update(ctx, updated))

	// Wait for NetworkPolicy deletion.
	requireEventually(t, func() bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-np-cleanup", Namespace: ns}, np)
		return apierrors.IsNotFound(err)
	}, "NetworkPolicy should be deleted after disabling")
}

func TestReconcile_NetworkPolicyNotCreatedByDefault(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newCluster("np-absent", ns)
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Wait for the main service to appear (proves reconciliation ran).
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-np-absent", Namespace: ns}, &corev1.Service{}) == nil
	}, "Service should be created")

	// NetworkPolicy should NOT exist.
	np := &networkingv1.NetworkPolicy{}
	err := k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-np-absent", Namespace: ns}, np)
	assert.True(t, apierrors.IsNotFound(err), "NetworkPolicy should not be created when spec.networkPolicy is nil")
}

func TestReconcile_NetworkPolicyCustomPorts(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newCluster("np-ports", ns)
	ls.Spec.Service.RaftPort = 17777
	ls.Spec.Service.GrpcPort = 18888
	ls.Spec.Service.HttpPort = 19000
	ls.Spec.NetworkPolicy = &ledgerv1alpha1.NetworkPolicySpec{
		Enabled: true,
	}
	require.NoError(t, k8sClient.Create(ctx, ls))

	np := &networkingv1.NetworkPolicy{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-np-ports", Namespace: ns}, np) == nil
	}, "NetworkPolicy should be created")

	require.Len(t, np.Spec.Egress, 3)
	interNode := np.Spec.Egress[0]
	require.Len(t, interNode.Ports, 3)
	assert.Equal(t, int32(17777), interNode.Ports[0].Port.IntVal)
	assert.Equal(t, int32(18888), interNode.Ports[1].Port.IntVal)
	assert.Equal(t, int32(19000), interNode.Ports[2].Port.IntVal)
}

func TestReconcile_NetworkPolicyUpdate(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newCluster("np-update", ns)
	ls.Spec.NetworkPolicy = &ledgerv1alpha1.NetworkPolicySpec{
		Enabled: true,
	}
	require.NoError(t, k8sClient.Create(ctx, ls))

	np := &networkingv1.NetworkPolicy{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-np-update", Namespace: ns}, np) == nil
	}, "NetworkPolicy should be created")

	// Verify default CIDR except.
	require.Len(t, np.Spec.Egress, 3)
	assert.Equal(t, defaultExternalCIDRExcept, np.Spec.Egress[2].To[0].IPBlock.Except)

	// Update to custom CIDR.
	updated := &ledgerv1alpha1.Cluster{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "np-update", Namespace: ns}, updated))
	updated.Spec.NetworkPolicy.ExternalCIDRExcept = []string{"10.0.0.0/8"}
	require.NoError(t, k8sClient.Update(ctx, updated))

	// Wait for CIDR to be updated.
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-np-update", Namespace: ns}, np); err != nil {
			return false
		}
		return len(np.Spec.Egress[2].To[0].IPBlock.Except) == 1
	}, "NetworkPolicy CIDR except should be updated")

	assert.Equal(t, []string{"10.0.0.0/8"}, np.Spec.Egress[2].To[0].IPBlock.Except)
}

