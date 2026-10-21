package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// clusterNamed returns a Cluster with the given CR name and no
// spec.resourceName, i.e. the default "ledger-<name>" base name.
func clusterNamed(name string) *ledgerv1alpha1.Cluster {
	return &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

// controlledByCluster stamps the controller reference the operator sets on
// every object it creates (CreateOrUpdate + SetControllerReference), so test
// fixtures seed derived objects as the operator would actually have left them.
// It also gives the object the UID the API server assigns on create (the fake
// client assigns none): guarded deletes are pinned to the inspected UID
// (deleteInspected) and refuse an object without one.
func controlledByCluster[T metav1.Object](obj T, cluster *ledgerv1alpha1.Cluster) T {
	obj.SetOwnerReferences(append(obj.GetOwnerReferences(),
		*metav1.NewControllerRef(cluster, ledgerv1alpha1.GroupVersion.WithKind("Cluster"))))
	if obj.GetUID() == "" {
		obj.SetUID(types.UID(obj.GetName() + "-uid"))
	}

	return obj
}

// clusterWithResourceName returns a Cluster whose base name is overridden by
// spec.resourceName.
func clusterWithResourceName(name, resourceName string) *ledgerv1alpha1.Cluster {
	cluster := clusterNamed(name)
	cluster.Spec.ResourceName = resourceName

	return cluster
}

func TestNameHelpers(t *testing.T) {
	t.Parallel()

	defaulted := clusterNamed("foo")
	overridden := clusterWithResourceName("jdxmvkvwlyiy-cukg", "payments")

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"clusterBaseName", clusterBaseName(defaulted), "ledger-foo"},
		{"headlessServiceName", headlessServiceName(defaulted), "ledger-foo-headless"},
		{"grpcServiceName", grpcServiceName(defaulted), "ledger-foo-grpc"},
		{"grpcIngressName", grpcIngressName(defaulted), "ledger-foo-grpc"},
		{"authKeysConfigMapName", authKeysConfigMapName(defaulted), "ledger-foo-auth-keys"},
		{"clusterSecretName", clusterSecretName(defaulted), "ledger-foo-cluster-secret"},
		{"dnsEndpointName", dnsEndpointName(defaulted, "public"), "ledger-foo-public"},
		{"serviceAccountName", serviceAccountName(defaulted), "ledger-foo"},
		{"podName-0", podName(defaulted, 0), "ledger-foo-0"},
		{"podName-2", podName(defaulted, 2), "ledger-foo-2"},
		{"pvcName", pvcName(defaulted, "data", 1), "data-ledger-foo-1"},

		// spec.resourceName replaces the whole base name: no "ledger-" prefix
		// and no CR name.
		{"override/clusterBaseName", clusterBaseName(overridden), "payments"},
		{"override/headlessServiceName", headlessServiceName(overridden), "payments-headless"},
		{"override/grpcServiceName", grpcServiceName(overridden), "payments-grpc"},
		{"override/grpcIngressName", grpcIngressName(overridden), "payments-grpc"},
		{"override/authKeysConfigMapName", authKeysConfigMapName(overridden), "payments-auth-keys"},
		{"override/clusterSecretName", clusterSecretName(overridden), "payments-cluster-secret"},
		{"override/dnsEndpointName", dnsEndpointName(overridden, "public"), "payments-public"},
		{"override/serviceAccountName", serviceAccountName(overridden), "payments"},
		{"override/podName-0", podName(overridden, 0), "payments-0"},
		{"override/pvcName", pvcName(overridden, "wal", 2), "wal-payments-2"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.want)
		}
	}
}

// TestPrefixedNameUnaffectedByResourceName guards that objects not derived
// from a Cluster base name keep the plain prefix.
func TestPrefixedNameUnaffectedByResourceName(t *testing.T) {
	t.Parallel()

	if got := credentialsCanonicalSecretName("creds"); got != "ledger-creds-credentials-canonical" {
		t.Errorf("credentialsCanonicalSecretName = %q", got)
	}
	if got := prefixedName("run"); got != "ledger-run" {
		t.Errorf("prefixedName = %q", got)
	}
}

// TestScaleDownPodNameMatchesStatefulSet guards the EN-1319 regression where
// scale-down dialed bare-CR pod names (foo-N) while the StatefulSet — and thus
// its pods — is clusterBaseName(cr) (ledger-foo-N). The pod name for any
// ordinal must equal the StatefulSet name plus the ordinal, with or without a
// spec.resourceName override.
func TestScaleDownPodNameMatchesStatefulSet(t *testing.T) {
	t.Parallel()

	for _, cluster := range []*ledgerv1alpha1.Cluster{clusterNamed("foo"), clusterWithResourceName("foo", "bar")} {
		if got, want := podName(cluster, 2), clusterBaseName(cluster)+"-2"; got != want {
			t.Fatalf("podName(%+v,2) = %q, want %q", cluster.Spec.ResourceName, got, want)
		}
	}
	if got, want := podName(clusterNamed("foo"), 0), "ledger-foo-0"; got != want {
		t.Fatalf("podName(foo,0) = %q, want %q", got, want)
	}
}

func TestServiceAccountName(t *testing.T) {
	t.Parallel()

	defaulted := &ledgerv1alpha1.Cluster{}
	defaulted.Name = "foo"
	if got := serviceAccountName(defaulted); got != "ledger-foo" {
		t.Errorf("default serviceAccountName = %q, want %q", got, "ledger-foo")
	}

	overridden := &ledgerv1alpha1.Cluster{}
	overridden.Name = "foo"
	overridden.Spec.ServiceAccount.Name = "custom-sa"
	overridden.Spec.ResourceName = "bar"
	if got := serviceAccountName(overridden); got != "custom-sa" {
		t.Errorf("overridden serviceAccountName = %q, want %q (verbatim)", got, "custom-sa")
	}
}

// TestBootstrapDNSConsistency guards that the two halves of the raft bootstrap
// DNS address — podName(cluster, 0) and headlessServiceName(cluster) — both
// derive from the same StatefulSet base (clusterBaseName). A mismatch would
// produce an unresolvable DNS name at cluster bootstrap.
func TestBootstrapDNSConsistency(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		cluster  *ledgerv1alpha1.Cluster
		pod0     string
		headless string
	}{
		{clusterNamed("foo"), "ledger-foo-0", "ledger-foo-headless"},
		{clusterWithResourceName("foo", "bar"), "bar-0", "bar-headless"},
	} {
		pod0 := podName(tc.cluster, 0)
		hls := headlessServiceName(tc.cluster)
		if pod0 != tc.pod0 {
			t.Fatalf("pod0 = %q, want %q", pod0, tc.pod0)
		}
		if hls != tc.headless {
			t.Fatalf("headless = %q, want %q", hls, tc.headless)
		}
		if want := clusterBaseName(tc.cluster); pod0 != want+"-0" || hls != want+"-headless" {
			t.Fatalf("pod0=%q hls=%q must both derive from %q", pod0, hls, want)
		}
	}
}
