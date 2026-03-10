package controller

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

// computeSpecHash returns a SHA-256 hash of the serialized LedgerService spec,
// excluding fields that don't affect the pod template.
// This is used as a pod template annotation to trigger rolling updates on spec changes.
//
// Fields that map to separate Kubernetes resources (Service, Ingress, NetworkPolicy,
// DNSEndpoint, ServiceMonitor, PDB) are excluded because changes to them do not
// require a pod restart.
func computeSpecHash(spec *ledgerv1alpha1.LedgerServiceSpec) string {
	// Copy the spec and nil out fields that should not trigger a rolling update.
	cp := *spec

	// Replicas: changing count only adds/removes pods; existing pods don't need restart.
	cp.Replicas = nil

	// Networking resources (reconciled as separate K8s objects).
	cp.Service = ledgerv1alpha1.ServiceSpec{}
	cp.HeadlessService = ledgerv1alpha1.HeadlessServiceSpec{}
	cp.Ingress = nil
	cp.IngressGrpc = nil
	cp.NetworkPolicy = nil
	cp.DNSEndpoint = nil
	cp.AutoNetworking = nil

	// Monitoring resources (reconciled as separate K8s objects).
	cp.ServiceMonitor = nil
	cp.PodDisruptionBudget = nil

	data, _ := json.Marshal(&cp) //nolint:errchkjson // spec is always serializable

	return fmt.Sprintf("%x", sha256.Sum256(data))
}

// computeAuthKeysHash returns a SHA-256 hash of the sorted agent key info list.
// This is used as a pod template annotation to trigger rolling updates when
// the set of auth keys changes.
func computeAuthKeysHash(agents []agentKeyInfo) string {
	data, _ := json.Marshal(agents) //nolint:errchkjson // agents is always serializable

	return fmt.Sprintf("%x", sha256.Sum256(data))
}
