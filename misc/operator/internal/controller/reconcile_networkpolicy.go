package controller

import (
	"context"
	"maps"
	"net/url"
	"slices"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

var defaultExternalCIDRExcept = []string{
	"10.0.0.0/8",
	"172.16.0.0/12",
	"192.168.0.0/16",
}

func (r *LedgerServiceReconciler) reconcileNetworkPolicy(ctx context.Context, ledger *ledgerv1alpha1.LedgerService) error {
	name := resourceName(ledger.Name)

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

	egress := []networkingv1.NetworkPolicyEgressRule{
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
	}

	if rule := otelEgressRule(ledger); rule != nil {
		egress = append(egress, *rule)
	}

	egress = append(egress, ledger.Spec.NetworkPolicy.AdditionalEgress...)

	return networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{
			MatchLabels: selectorLabels(ledger),
		},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
		Egress:      egress,
	}
}

func otelEgressRule(ledger *ledgerv1alpha1.LedgerService) *networkingv1.NetworkPolicyEgressRule {
	mon := ledger.Spec.Monitoring
	if mon == nil {
		return nil
	}

	type exporter struct {
		enabled *bool
		port    string
	}
	exporters := []exporter{
		{enabled: enabledFromTraces(mon.Traces), port: portFromTraces(mon.Traces)},
		{enabled: enabledFromMetrics(mon.Metrics), port: portFromMetrics(mon.Metrics)},
		{enabled: enabledFromLogs(mon.Logs), port: portFromLogs(mon.Logs)},
	}

	anyEnabled := false
	ports := make(map[int32]struct{})
	for _, e := range exporters {
		if e.enabled == nil || !*e.enabled {
			continue
		}
		anyEnabled = true
		if p, err := strconv.ParseInt(e.port, 10, 32); err == nil && p > 0 {
			ports[int32(p)] = struct{}{}
		}
	}

	// Include Pyroscope port if enabled.
	if mon.Pyroscope != nil && mon.Pyroscope.Enabled {
		anyEnabled = true

		if p := portFromServerAddress(mon.Pyroscope.ServerAddress); p > 0 {
			ports[p] = struct{}{}
		}
	}

	if !anyEnabled {
		return nil
	}

	// Default OTEL ports when no custom port was configured.
	if len(ports) == 0 {
		ports[4317] = struct{}{}
		ports[4318] = struct{}{}
	}

	sorted := slices.Sorted(maps.Keys(ports))

	tcp := corev1.ProtocolTCP
	var npPorts []networkingv1.NetworkPolicyPort
	for _, p := range sorted {
		v := intstr.FromInt32(p)
		npPorts = append(npPorts, networkingv1.NetworkPolicyPort{Port: &v, Protocol: &tcp})
	}

	return &networkingv1.NetworkPolicyEgressRule{
		To: []networkingv1.NetworkPolicyPeer{
			{NamespaceSelector: &metav1.LabelSelector{}},
		},
		Ports: npPorts,
	}
}

func enabledFromTraces(t *ledgerv1alpha1.TracesConfig) *bool {
	if t == nil {
		return nil
	}

	return t.Enabled
}

func enabledFromMetrics(m *ledgerv1alpha1.MetricsConfig) *bool {
	if m == nil {
		return nil
	}

	return m.Enabled
}

func enabledFromLogs(l *ledgerv1alpha1.LogsConfig) *bool {
	if l == nil {
		return nil
	}

	return l.Enabled
}

func portFromTraces(t *ledgerv1alpha1.TracesConfig) string {
	if t == nil {
		return ""
	}

	return t.Port
}

func portFromMetrics(m *ledgerv1alpha1.MetricsConfig) string {
	if m == nil {
		return ""
	}

	return m.Port
}

func portFromLogs(l *ledgerv1alpha1.LogsConfig) string {
	if l == nil {
		return ""
	}

	return l.Port
}

// portFromServerAddress extracts the port from a URL like "http://host:4040".
func portFromServerAddress(addr string) int32 {
	if addr == "" {
		return 0
	}

	u, err := url.Parse(addr)
	if err != nil {
		return 0
	}

	portStr := u.Port()
	if portStr == "" {
		return 0
	}

	p, err := strconv.ParseInt(portStr, 10, 32)
	if err != nil {
		return 0
	}

	return int32(p)
}
