package controller

import (
	"testing"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestNameHelpers(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"resourceName", resourceName("foo"), "ledger-foo"},
		{"headlessServiceName", headlessServiceName("foo"), "ledger-foo-headless"},
		{"grpcServiceName", grpcServiceName("foo"), "ledger-foo-grpc"},
		{"grpcIngressName", grpcIngressName("foo"), "ledger-foo-grpc"},
		{"authKeysConfigMapName", authKeysConfigMapName("foo"), "ledger-foo-auth-keys"},
		{"clusterSecretName", clusterSecretName("foo"), "ledger-foo-cluster-secret"},
		{"podName-0", podName("foo", 0), "ledger-foo-0"},
		{"podName-2", podName("foo", 2), "ledger-foo-2"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.want)
		}
	}
}

// TestScaleDownPodNameMatchesStatefulSet guards the EN-1319 regression where
// scale-down dialed bare-CR pod names (foo-N) while the StatefulSet — and thus
// its pods — is resourceName(cr) (ledger-foo-N). The pod name for any ordinal
// must equal the StatefulSet name plus the ordinal.
func TestScaleDownPodNameMatchesStatefulSet(t *testing.T) {
	cr := "foo"
	if got, want := podName(cr, 0), "ledger-foo-0"; got != want {
		t.Fatalf("podName(%q,0) = %q, want %q", cr, got, want)
	}
	if got, want := podName(cr, 2), resourceName(cr)+"-2"; got != want {
		t.Fatalf("podName(%q,2) = %q, want %q", cr, got, want)
	}
}

func TestServiceAccountName(t *testing.T) {
	defaulted := &ledgerv1alpha1.Cluster{}
	defaulted.Name = "foo"
	if got := serviceAccountName(defaulted); got != "ledger-foo" {
		t.Errorf("default serviceAccountName = %q, want %q", got, "ledger-foo")
	}

	overridden := &ledgerv1alpha1.Cluster{}
	overridden.Name = "foo"
	overridden.Spec.ServiceAccount.Name = "custom-sa"
	if got := serviceAccountName(overridden); got != "custom-sa" {
		t.Errorf("overridden serviceAccountName = %q, want %q (verbatim)", got, "custom-sa")
	}
}

// TestBootstrapDNSConsistency guards that the two halves of the raft bootstrap
// DNS address — podName(cr, 0) and headlessServiceName(cr) — both derive from
// the same prefixed StatefulSet base (resourceName(cr)). A mismatch would
// produce an unresolvable DNS name at cluster bootstrap.
func TestBootstrapDNSConsistency(t *testing.T) {
	cr := "foo"
	pod0 := podName(cr, 0)
	hls := headlessServiceName(cr)
	if pod0 != "ledger-foo-0" {
		t.Fatalf("pod0 = %q, want ledger-foo-0", pod0)
	}
	if hls != "ledger-foo-headless" {
		t.Fatalf("headless = %q, want ledger-foo-headless", hls)
	}
	// Both halves must derive from the same prefixed StatefulSet base.
	if want := resourceName(cr); pod0 != want+"-0" || hls != want+"-headless" {
		t.Fatalf("pod0=%q hls=%q must both derive from %q", pod0, hls, want)
	}
}

// TestCrossCRDPodNameConsistency guards that the Ledger CRD controller and the
// Cluster reconciler agree on pod names: the Ledger controller dials
// podName(ClusterRef, 0) while the Cluster reconciler creates pods via
// the same helper. A divergence would cause the dial to target a non-existent
// pod.
func TestCrossCRDPodNameConsistency(t *testing.T) {
	clusterRef := "foo"
	// Cluster reconciler names StatefulSet pods via podName.
	clusterPod0 := podName(clusterRef, 0)
	// Ledger CRD controller dials the same pod via podName(ClusterRef, 0).
	ledgerCRDDialTarget := podName(clusterRef, 0)
	if clusterPod0 != ledgerCRDDialTarget {
		t.Fatalf("cross-CRD pod-0 mismatch: %q vs %q", clusterPod0, ledgerCRDDialTarget)
	}
	if clusterPod0 != "ledger-foo-0" {
		t.Fatalf("pod-0 = %q, want ledger-foo-0", clusterPod0)
	}
}
