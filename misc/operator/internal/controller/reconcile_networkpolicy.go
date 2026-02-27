package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

var defaultExternalCIDRExcept = []string{
	"10.0.0.0/8",
	"172.16.0.0/12",
	"192.168.0.0/16",
}

func (r *LedgerServiceReconciler) reconcileNetworkPolicy(ctx context.Context, ledger *ledgerv1alpha1.LedgerService) error {
	name := ledger.Name

	if ledger.Spec.NetworkPolicy == nil || !ledger.Spec.NetworkPolicy.Enabled {
		return r.deleteIfExists(ctx, &networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ledger.Namespace,
			},
		})
	}

	np := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ledger.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, np, func() error {
		np.Labels = commonLabels(ledger)
		np.Spec = buildNetworkPolicySpec(ledger)
		return controllerutil.SetControllerReference(ledger, np, r.Scheme)
	})
	return err
}

func buildNetworkPolicySpec(ledger *ledgerv1alpha1.LedgerService) networkingv1.NetworkPolicySpec {
	except := defaultExternalCIDRExcept
	if len(ledger.Spec.NetworkPolicy.ExternalCIDRExcept) > 0 {
		except = ledger.Spec.NetworkPolicy.ExternalCIDRExcept
	}

	raftPort := intstr.FromInt32(serviceRaftPort(ledger))
	grpcPort := intstr.FromInt32(serviceGrpcPort(ledger))
	httpPort := intstr.FromInt32(serviceHttpPort(ledger))
	dnsPort := intstr.FromInt32(53)
	tcp := corev1.ProtocolTCP
	udp := corev1.ProtocolUDP

	return networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{
			MatchLabels: selectorLabels(ledger),
		},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
		Egress: []networkingv1.NetworkPolicyEgressRule{
			{
				// Inter-node communication (Raft, gRPC, HTTP).
				To: []networkingv1.NetworkPolicyPeer{
					{
						PodSelector: &metav1.LabelSelector{
							MatchLabels: selectorLabels(ledger),
						},
					},
				},
				Ports: []networkingv1.NetworkPolicyPort{
					{Port: &raftPort, Protocol: &tcp},
					{Port: &grpcPort, Protocol: &tcp},
					{Port: &httpPort, Protocol: &tcp},
				},
			},
			{
				// DNS resolution (kube-dns in any namespace).
				To: []networkingv1.NetworkPolicyPeer{
					{
						NamespaceSelector: &metav1.LabelSelector{},
						PodSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"k8s-app": "kube-dns"},
						},
					},
				},
				Ports: []networkingv1.NetworkPolicyPort{
					{Port: &dnsPort, Protocol: &udp},
					{Port: &dnsPort, Protocol: &tcp},
				},
			},
			{
				// External egress (non-RFC1918).
				To: []networkingv1.NetworkPolicyPeer{
					{
						IPBlock: &networkingv1.IPBlock{
							CIDR:   "0.0.0.0/0",
							Except: except,
						},
					},
				},
			},
		},
	}
}
