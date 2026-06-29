package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestSelectorLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		spec     ledgerv1alpha1.LedgerServiceSpec
		instance string
		want     map[string]string
	}{
		{
			name:     "defaults only",
			instance: "my-ledger",
			want: map[string]string{
				"app.kubernetes.io/name":     "ledger",
				"app.kubernetes.io/instance": "my-ledger",
			},
		},
		{
			name:     "additive labels merged in",
			instance: "my-ledger",
			spec: ledgerv1alpha1.LedgerServiceSpec{
				AdditionalLabels: map[string]string{
					"app.formance.com/service": "ledger-v3",
					"team":                     "platform",
				},
			},
			want: map[string]string{
				"app.kubernetes.io/name":     "ledger",
				"app.kubernetes.io/instance": "my-ledger",
				"app.formance.com/service":   "ledger-v3",
				"team":                       "platform",
			},
		},
		{
			name:     "additional labels override the default name",
			instance: "my-ledger",
			spec: ledgerv1alpha1.LedgerServiceSpec{
				AdditionalLabels: map[string]string{
					"app.kubernetes.io/name": "ledger-v3",
				},
			},
			want: map[string]string{
				"app.kubernetes.io/name":     "ledger-v3",
				"app.kubernetes.io/instance": "my-ledger",
			},
		},
		{
			// selectorLabels feeds the StatefulSet pod template; managed-by
			// MUST stay out of it so the operator's ownership label cannot
			// be hijacked through additionalLabels (NumaryBot finding on PR
			// #578).
			name:     "managed-by in additionalLabels is dropped from selector",
			instance: "my-ledger",
			spec: ledgerv1alpha1.LedgerServiceSpec{
				AdditionalLabels: map[string]string{
					"app.kubernetes.io/managed-by": "evil",
					"app.formance.com/service":     "ledger-v3",
				},
			},
			want: map[string]string{
				"app.kubernetes.io/name":     "ledger",
				"app.kubernetes.io/instance": "my-ledger",
				"app.formance.com/service":   "ledger-v3",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ls := &ledgerv1alpha1.LedgerService{
				ObjectMeta: metav1.ObjectMeta{Name: tt.instance},
				Spec:       tt.spec,
			}
			require.Equal(t, tt.want, selectorLabels(ls))
		})
	}
}

func TestCommonLabels_ManagedByIsNotOverridable(t *testing.T) {
	t.Parallel()

	ls := &ledgerv1alpha1.LedgerService{
		ObjectMeta: metav1.ObjectMeta{Name: "my-ledger"},
		Spec: ledgerv1alpha1.LedgerServiceSpec{
			AdditionalLabels: map[string]string{
				// User tries to hijack ownership tracking; we ignore it.
				"app.kubernetes.io/managed-by": "evil",
			},
		},
	}

	got := commonLabels(ls)
	require.Equal(t, "ledger-operator", got["app.kubernetes.io/managed-by"],
		"managed-by is owned by the operator and must not be overridable")
}
