package controller

import (
	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

const (
	labelManagedBy         = "app.kubernetes.io/managed-by"
	labelInstance          = "app.kubernetes.io/instance"
	labelName              = "app.kubernetes.io/name"
	annotationSpecHash     = "ledger.formance.com/spec-hash"
	annotationAuthKeysHash = "ledger.formance.com/auth-keys-hash"
)

// selectorLabels returns the labels used to select pods owned by this LedgerService.
//
// The base set is the two app.kubernetes.io/{name,instance} labels. Any entry
// in spec.additionalLabels is merged on top and may override a default key
// (use case: the default name=ledger is matched by an unrelated Service in
// the namespace, so the user widens our name to "ledger-v3"). managed-by is
// dropped from the merge here: selectorLabels feeds the StatefulSet pod
// template labels, so accepting a user override would leak into pods and
// contradict the documented non-overridable ownership label that commonLabels
// guarantees on top-level objects.
func selectorLabels(ledger *ledgerv1alpha1.LedgerService) map[string]string {
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

// commonLabels returns labels applied to all resources owned by this LedgerService.
//
// managed-by is written last and is NOT overridable: it is how the operator
// tracks ownership across reconciles.
func commonLabels(ledger *ledgerv1alpha1.LedgerService) map[string]string {
	labels := selectorLabels(ledger)
	labels[labelManagedBy] = "ledger-operator"

	return labels
}
