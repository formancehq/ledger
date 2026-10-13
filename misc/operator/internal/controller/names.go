package controller

import (
	"strconv"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// resourcePrefix is prepended to the name of every Kubernetes object the
// operator creates, so operator-owned resources cannot collide with same-named
// resources from other products deployed in the same namespace (EN-1319).
// A Cluster may replace its whole base name with spec.resourceName, in which
// case the prefix is not applied to that Cluster's objects (see clusterBaseName).
const resourcePrefix = "ledger-"

// dns1035LabelMaxLength is the RFC 1035 label cap (63 chars) that bounds every
// Service name and pod name the operator derives from a Cluster's base name.
// Also the cap on any Kubernetes label value.
const dns1035LabelMaxLength = 63

// statefulSetNameMaxLength is the longest StatefulSet name — and therefore the
// longest Cluster base name (clusterBaseName) — whose pods can be created. The
// StatefulSet controller labels every pod with
// controller-revision-hash=<statefulset>-<hash>, where the hash is up to 10
// characters; a label value is at most 63 characters, so the name is capped at
// 63 - len("-") - 10 = 52 (kubernetes#64023). It is tighter than every other
// derived-name bound (e.g. "<base>-headless" = 61 chars), so validateSpec and
// the spec.resourceName CRD MaxLength enforce it on the base name.
const statefulSetNameMaxLength = dns1035LabelMaxLength - len("-") - 10

// prefixedName applies the resource prefix to a CR name. It names objects
// that are not derived from a Cluster base name (Credentials secrets, backup
// Jobs) and is the default Cluster base name. Never use it to re-derive the
// name of an object belonging to a Cluster: go through clusterBaseName.
func prefixedName(crName string) string {
	return resourcePrefix + crName
}

// clusterBaseName is the single source of truth for the base name of every
// object derived from a Cluster: spec.resourceName when set, otherwise the
// prefixed CR name. It is also the name of the StatefulSet, ClusterIP Service,
// Ingress, NetworkPolicy and default ServiceAccount. Every derived name below,
// and every reference that re-derives one (pod DNS, Service DNS, PVC names,
// cross-CRD pod-0 dials), takes the Cluster object so it cannot bypass the
// override.
func clusterBaseName(cluster *ledgerv1alpha1.Cluster) string {
	if cluster.Spec.ResourceName != "" {
		return cluster.Spec.ResourceName
	}

	return prefixedName(cluster.Name)
}

// dnsEndpointName returns the DNSEndpoint object name for the entry identified
// by epName within a Cluster. Each spec.dnsEndpoints entry is reconciled into
// its own DNSEndpoint object, so its name is suffixed with the entry's unique
// name to keep the objects distinct.
func dnsEndpointName(cluster *ledgerv1alpha1.Cluster, epName string) string {
	return clusterBaseName(cluster) + "-" + epName
}

// headlessServiceName returns the headless Service name for a Cluster.
// It is also the StatefulSet ServiceName and the DNS subdomain of every pod.
func headlessServiceName(cluster *ledgerv1alpha1.Cluster) string {
	return clusterBaseName(cluster) + "-headless"
}

// grpcServiceName returns the dedicated gRPC Service name for a Cluster.
func grpcServiceName(cluster *ledgerv1alpha1.Cluster) string {
	return clusterBaseName(cluster) + "-grpc"
}

// grpcIngressName returns the gRPC Ingress object name for a Cluster.
func grpcIngressName(cluster *ledgerv1alpha1.Cluster) string {
	return clusterBaseName(cluster) + "-grpc"
}

// authKeysConfigMapName returns the auth-keys ConfigMap name for a Cluster.
func authKeysConfigMapName(cluster *ledgerv1alpha1.Cluster) string {
	return clusterBaseName(cluster) + "-auth-keys"
}

// clusterSecretName returns the cluster inter-node Secret name for a Cluster.
func clusterSecretName(cluster *ledgerv1alpha1.Cluster) string {
	return clusterBaseName(cluster) + "-cluster-secret"
}

// credentialsCanonicalSecretName returns the name of the canonical seed Secret for a
// Credentials. This Secret lives in the operator's own namespace and
// holds the sole source of truth for the credentials's Ed25519 keypair; the
// per-target replicas are pure projections of its content.
func credentialsCanonicalSecretName(credentialsName string) string {
	return prefixedName(credentialsName) + "-credentials-canonical"
}

// podName returns the StatefulSet pod name for the given ordinal. The StatefulSet
// name is clusterBaseName(cluster), so its pods are clusterBaseName(cluster)-<ordinal>.
func podName(cluster *ledgerv1alpha1.Cluster, ordinal int) string {
	return clusterBaseName(cluster) + "-" + strconv.Itoa(ordinal)
}

// pvcName returns the name the StatefulSet controller gives the PVC of the
// given volume claim template and ordinal: <volume>-<statefulset>-<ordinal>.
func pvcName(cluster *ledgerv1alpha1.Cluster, volume string, ordinal int32) string {
	return volume + "-" + podName(cluster, int(ordinal))
}

// serviceAccountName returns the ServiceAccount name: the user-supplied override
// verbatim if set, otherwise the Cluster base name.
func serviceAccountName(cluster *ledgerv1alpha1.Cluster) string {
	if cluster.Spec.ServiceAccount.Name != "" {
		return cluster.Spec.ServiceAccount.Name
	}

	return clusterBaseName(cluster)
}
