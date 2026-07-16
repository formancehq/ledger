package controller

import (
	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const (
	labelManagedBy         = "app.kubernetes.io/managed-by"
	labelInstance          = "app.kubernetes.io/instance"
	labelName              = "app.kubernetes.io/name"
	annotationSpecHash     = "ledger.formance.com/spec-hash"
	annotationAuthKeysHash = "ledger.formance.com/auth-keys-hash"

	// labelDeletionProtection (set to labelDeletionProtectionValue) is stamped on
	// the PVCs and bound PVs of a Cluster whose spec.persistence.deletionProtection
	// is true. The volume deletion-protection ValidatingAdmissionPolicyBinding scopes
	// itself to this label, so a ledger opts in or out per-CR without touching the
	// cluster-scoped policy. PVs are cluster-scoped and don't inherit PVC labels, so
	// the operator stamps both sides explicitly.
	labelDeletionProtection      = "ledger.formance.com/deletion-protection"
	labelDeletionProtectionValue = "enabled"

	// volumeProtectionPVCBindingName is the fixed, release-independent name of the
	// PVC ValidatingAdmissionPolicyBinding rendered by the chart when
	// pvcProtection.enabled=true (see helm/operator/templates/validatingadmissionpolicy.yaml).
	// The controller probes it to tell whether deletion protection is actually
	// active cluster-wide — which is what matters in a multi-release setup where a
	// sibling release owns the singleton — rather than trusting this release's own
	// Helm flag. The PV binding is installed in the same bundle, so the PVC binding's
	// presence is a sufficient proxy for the whole policy.
	volumeProtectionPVCBindingName = "ledger-volume-protection-pvc"
)

// selectorLabels returns the labels used to select pods owned by this Cluster.
//
// The base set is the two app.kubernetes.io/{name,instance} labels. Any entry
// in spec.additionalLabels is merged on top and may override a default key
// (use case: the default name=ledger is matched by an unrelated Service in
// the namespace, so the user widens our name to "ledger-v3"). managed-by is
// dropped from the merge here: selectorLabels feeds the StatefulSet pod
// template labels, so accepting a user override would leak into pods and
// contradict the documented non-overridable ownership label that commonLabels
// guarantees on top-level objects.
func selectorLabels(ledger *ledgerv1alpha1.Cluster) map[string]string {
	labels := map[string]string{
		labelName:     "ledger",
		labelInstance: ledger.Name,
	}
	for k, v := range ledger.Spec.AdditionalLabels {
		if k == labelManagedBy {
			continue
		}
		labels[k] = v
	}

	return labels
}

// commonLabels returns labels applied to all resources owned by this Cluster.
//
// managed-by is written last and is NOT overridable: it is how the operator
// tracks ownership across reconciles.
func commonLabels(ledger *ledgerv1alpha1.Cluster) map[string]string {
	labels := selectorLabels(ledger)
	labels[labelManagedBy] = "ledger-operator"

	return labels
}
