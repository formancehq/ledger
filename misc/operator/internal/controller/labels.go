package controller

import (
	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

const (
	labelManagedBy = "app.kubernetes.io/managed-by"
	labelInstance  = "app.kubernetes.io/instance"
	labelName      = "app.kubernetes.io/name"
	labelComponent = "app.kubernetes.io/component"

	annotationSpecHash = "ledger.formance.com/spec-hash"
)

// selectorLabels returns the labels used to select pods owned by this Ledger.
func selectorLabels(ledger *ledgerv1alpha1.Ledger) map[string]string {
	return map[string]string{
		labelName:     "ledger",
		labelInstance: ledger.Name,
	}
}

// commonLabels returns labels applied to all resources owned by this Ledger.
func commonLabels(ledger *ledgerv1alpha1.Ledger) map[string]string {
	labels := selectorLabels(ledger)
	labels[labelManagedBy] = "ledger-operator"
	return labels
}

// componentLabels returns labels with an additional component label.
func componentLabels(ledger *ledgerv1alpha1.Ledger, component string) map[string]string {
	labels := commonLabels(ledger)
	labels[labelComponent] = component
	return labels
}
