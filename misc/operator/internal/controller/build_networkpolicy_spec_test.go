package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func newTestLedgerService(name string, np *ledgerv1alpha1.NetworkPolicySpec) *ledgerv1alpha1.LedgerService {
	replicas := int32(3)
	return &ledgerv1alpha1.LedgerService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: ledgerv1alpha1.LedgerServiceSpec{
			Replicas:      &replicas,
			NetworkPolicy: np,
		},
	}
}

func TestBuildNetworkPolicySpec_DefaultCIDR(t *testing.T) {
	t.Parallel()

	ls := newTestLedgerService("test", &ledgerv1alpha1.NetworkPolicySpec{Enabled: true})
	spec := buildNetworkPolicySpec(ls)

	// PodSelector matches the LedgerService.
	assert.Equal(t, "ledger", spec.PodSelector.MatchLabels[labelName])
	assert.Equal(t, "test", spec.PodSelector.MatchLabels[labelInstance])

	// Egress-only policy.
	require.Len(t, spec.PolicyTypes, 1)
	assert.Equal(t, networkingv1.PolicyTypeEgress, spec.PolicyTypes[0])

	// 3 egress rules.
	require.Len(t, spec.Egress, 3)

	// External rule uses default RFC1918 except list.
	ext := spec.Egress[2]
	require.Len(t, ext.To, 1)
	require.NotNil(t, ext.To[0].IPBlock)
	assert.Equal(t, "0.0.0.0/0", ext.To[0].IPBlock.CIDR)
	assert.Equal(t, defaultExternalCIDRExcept, ext.To[0].IPBlock.Except)
}

func TestBuildNetworkPolicySpec_CustomCIDR(t *testing.T) {
	t.Parallel()

	custom := []string{"10.0.0.0/8", "100.64.0.0/10"}
	ls := newTestLedgerService("custom", &ledgerv1alpha1.NetworkPolicySpec{
		Enabled:            true,
		ExternalCIDRExcept: custom,
	})
	spec := buildNetworkPolicySpec(ls)

	ext := spec.Egress[2]
	assert.Equal(t, custom, ext.To[0].IPBlock.Except)
}

func TestBuildNetworkPolicySpec_InterNodeRule(t *testing.T) {
	t.Parallel()

	ls := newTestLedgerService("inter", &ledgerv1alpha1.NetworkPolicySpec{Enabled: true})
	spec := buildNetworkPolicySpec(ls)

	interNode := spec.Egress[0]
	require.Len(t, interNode.To, 1)
	require.NotNil(t, interNode.To[0].PodSelector)
	assert.Equal(t, "inter", interNode.To[0].PodSelector.MatchLabels[labelInstance])
	assert.Equal(t, "ledger", interNode.To[0].PodSelector.MatchLabels[labelName])

	// Default ports: raft=7777, grpc=8888, http=9000.
	require.Len(t, interNode.Ports, 3)
	tcp := corev1.ProtocolTCP
	assert.Equal(t, int32(7777), interNode.Ports[0].Port.IntVal)
	assert.Equal(t, &tcp, interNode.Ports[0].Protocol)
	assert.Equal(t, int32(8888), interNode.Ports[1].Port.IntVal)
	assert.Equal(t, &tcp, interNode.Ports[1].Protocol)
	assert.Equal(t, int32(9000), interNode.Ports[2].Port.IntVal)
	assert.Equal(t, &tcp, interNode.Ports[2].Protocol)
}

func TestBuildNetworkPolicySpec_InterNodeCustomPorts(t *testing.T) {
	t.Parallel()

	ls := newTestLedgerService("ports", &ledgerv1alpha1.NetworkPolicySpec{Enabled: true})
	ls.Spec.Service.RaftPort = 17777
	ls.Spec.Service.GrpcPort = 18888
	ls.Spec.Service.HttpPort = 19000
	spec := buildNetworkPolicySpec(ls)

	interNode := spec.Egress[0]
	require.Len(t, interNode.Ports, 3)
	assert.Equal(t, int32(17777), interNode.Ports[0].Port.IntVal)
	assert.Equal(t, int32(18888), interNode.Ports[1].Port.IntVal)
	assert.Equal(t, int32(19000), interNode.Ports[2].Port.IntVal)
}

func TestBuildNetworkPolicySpec_DNSRule(t *testing.T) {
	t.Parallel()

	ls := newTestLedgerService("dns", &ledgerv1alpha1.NetworkPolicySpec{Enabled: true})
	spec := buildNetworkPolicySpec(ls)

	dns := spec.Egress[1]
	require.Len(t, dns.To, 1)
	// NamespaceSelector is empty (matches all namespaces).
	require.NotNil(t, dns.To[0].NamespaceSelector)
	assert.Empty(t, dns.To[0].NamespaceSelector.MatchLabels)
	// PodSelector targets kube-dns.
	require.NotNil(t, dns.To[0].PodSelector)
	assert.Equal(t, "kube-dns", dns.To[0].PodSelector.MatchLabels["k8s-app"])

	// Ports: 53 UDP + 53 TCP.
	require.Len(t, dns.Ports, 2)
	udp := corev1.ProtocolUDP
	tcp := corev1.ProtocolTCP
	assert.Equal(t, int32(53), dns.Ports[0].Port.IntVal)
	assert.Equal(t, &udp, dns.Ports[0].Protocol)
	assert.Equal(t, int32(53), dns.Ports[1].Port.IntVal)
	assert.Equal(t, &tcp, dns.Ports[1].Protocol)
}
