package controller

import (
	"strconv"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// resourcePrefix is prepended to the name of every Kubernetes object the
// operator creates, so operator-owned resources cannot collide with same-named
// resources from other products deployed in the same namespace (EN-1319).
const resourcePrefix = "ledger-"

// dns1035LabelMaxLength is the RFC 1035 label cap (63 chars) that bounds every
// Service name and pod name the operator derives from a CR name. validateSpec
// rejects CR names whose tightest derived label (headlessServiceName) exceeds it.
const dns1035LabelMaxLength = 63

// prefixedName is the single source of truth for the resource prefix. Every
// operator-created object name, and every reference that re-derives such a name
// (pod DNS, service DNS, PVC names, cross-CRD pod-0 dials), routes through here.
func prefixedName(crName string) string {
	return resourcePrefix + crName
}

// resourceName is the base name for the primary objects of a Cluster:
// the StatefulSet, ClusterIP Service, Ingress and NetworkPolicy. DNSEndpoint
// objects derive their names from it via dnsEndpointName.
func resourceName(crName string) string {
	return prefixedName(crName)
}

// dnsEndpointName returns the DNSEndpoint object name for the entry identified
// by epName within a Cluster. Each spec.dnsEndpoints entry is reconciled into
// its own DNSEndpoint object, so its name is suffixed with the entry's unique
// name to keep the objects distinct.
func dnsEndpointName(crName, epName string) string {
	return resourceName(crName) + "-" + epName
}

// headlessServiceName returns the headless Service name for a Cluster.
// It is also the StatefulSet ServiceName and the DNS subdomain of every pod.
func headlessServiceName(crName string) string {
	return resourceName(crName) + "-headless"
}

// grpcServiceName returns the dedicated gRPC Service name for a Cluster.
func grpcServiceName(crName string) string {
	return resourceName(crName) + "-grpc"
}

// grpcIngressName returns the gRPC Ingress object name for a Cluster.
func grpcIngressName(crName string) string {
	return resourceName(crName) + "-grpc"
}

// authKeysConfigMapName returns the auth-keys ConfigMap name for a Cluster.
func authKeysConfigMapName(crName string) string {
	return resourceName(crName) + "-auth-keys"
}

// clusterSecretName returns the cluster inter-node Secret name for a Cluster.
func clusterSecretName(crName string) string {
	return resourceName(crName) + "-cluster-secret"
}

// credentialsCanonicalSecretName returns the name of the canonical seed Secret for a
// Credentials. This Secret lives in the operator's own namespace and
// holds the sole source of truth for the credentials's Ed25519 keypair; the
// per-target replicas are pure projections of its content.
func credentialsCanonicalSecretName(credentialsName string) string {
	return prefixedName(credentialsName) + "-credentials-canonical"
}

// podName returns the StatefulSet pod name for the given ordinal. The StatefulSet
// name is resourceName(crName), so its pods are resourceName(crName)-<ordinal>.
func podName(crName string, ordinal int) string {
	return resourceName(crName) + "-" + strconv.Itoa(ordinal)
}

// serviceAccountName returns the ServiceAccount name: the user-supplied override
// verbatim if set, otherwise the prefixed default.
func serviceAccountName(ledger *ledgerv1alpha1.Cluster) string {
	if ledger.Spec.ServiceAccount.Name != "" {
		return ledger.Spec.ServiceAccount.Name
	}

	return resourceName(ledger.Name)
}
