package controller

import (
	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

const (
	labelManagedBy         = "app.kubernetes.io/managed-by"
	labelInstance          = "app.kubernetes.io/instance"
	labelName              = "app.kubernetes.io/name"
	annotationSpecHash     = "ledger.formance.com/spec-hash"
	annotationAuthKeysHash = "ledger.formance.com/auth-keys-hash"
)

// selectorLabels returns the labels used to select pods owned by this LedgerService.
func selectorLabels(ledger *ledgerv1alpha1.LedgerService) map[string]string {
	return map[string]string{
		labelName:     "ledger",
		labelInstance: ledger.Name,
	}
}

// commonLabels returns labels applied to all resources owned by this LedgerService.
func commonLabels(ledger *ledgerv1alpha1.LedgerService) map[string]string {
	labels := selectorLabels(ledger)
	labels[labelManagedBy] = "ledger-operator"

	return labels
}
