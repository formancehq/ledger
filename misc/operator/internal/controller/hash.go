package controller

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// computeSpecHash returns a SHA-256 hash of the serialized Cluster spec,
// excluding fields that don't affect the pod template.
// This is used as a pod template annotation to trigger rolling updates on spec changes.
//
// Fields that map to separate Kubernetes resources (Service, Ingress, NetworkPolicy,
// DNSEndpoint) are excluded because changes to them do not require a pod restart.
func computeSpecHash(spec *ledgerv1alpha1.ClusterSpec) string {
	return computePodSpecHash(spec, true)
}

// computePodSpecHash also recognizes the pre-storage-reconciliation hash
// during migration. The legacy variant is never used for new Pod templates.
func computePodSpecHash(spec *ledgerv1alpha1.ClusterSpec, omitClaimSettings bool) string {
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

	// DeletionProtection and AutoExpansion only drive PVC/PV mutations in their
	// dedicated reconcilers; they have no pod-template effect, so tuning them
	// must not roll the StatefulSet. Persistence and its VolumeSpecs are value
	// fields, so zeroing these pointers on the shallow copy is safe.
	cp.Persistence.DeletionProtection = nil
	cp.Persistence.WAL.AutoExpansion = nil
	cp.Persistence.Data.AutoExpansion = nil
	// Claim declarations are reconciled or diagnosed on PVCs and immutable
	// templates. Capacity, profile, StorageClass and access mode do not change
	// the Pod's volume references or mounts. Keep HostPath hashed: it changes
	// Pod wiring. Keep ServiceAccount hashed: annotations affect Pod admission.
	if omitClaimSettings {
		cp.Persistence.WAL.Size = resource.Quantity{}
		cp.Persistence.Data.Size = resource.Quantity{}
		cp.Persistence.WAL.VolumeAttributesClassName = ""
		cp.Persistence.Data.VolumeAttributesClassName = ""
		cp.Persistence.WAL.StorageClass = ""
		cp.Persistence.Data.StorageClass = ""
		cp.Persistence.WAL.AccessMode = ""
		cp.Persistence.Data.AccessMode = ""
	}

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
