package controller

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// computeSpecHash returns a SHA-256 hash of the serialized Cluster spec,
// excluding fields that don't affect the pod template.
// This is used as a pod template annotation to trigger rolling updates on spec changes.
//
// Fields that map to separate Kubernetes resources (Service, Ingress, NetworkPolicy,
// DNSEndpoint) are excluded because changes to them do not require a pod restart.
func computeSpecHash(spec *ledgerv1alpha1.ClusterSpec) string {
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
	cp.DNSEndpoints = nil

	// DeletionProtection only drives PVC/PV label patches (reconcileVolumeProtection);
	// it has no pod-template effect, so toggling it must not roll the StatefulSet.
	// Persistence is a value field, so zeroing it on the shallow copy is safe.
	cp.Persistence.DeletionProtection = nil

	data, _ := json.Marshal(&cp) //nolint:errchkjson // spec is always serializable

	return fmt.Sprintf("%x", sha256.Sum256(data))
}

// computeAuthKeysHash returns a SHA-256 hash of the sorted credentials key info list.
// This is used as a pod template annotation to trigger rolling updates when
// the set of auth keys changes.
func computeAuthKeysHash(credentials []credentialsKeyInfo) string {
	data, _ := json.Marshal(credentials) //nolint:errchkjson // credentials is always serializable

	return fmt.Sprintf("%x", sha256.Sum256(data))
}
