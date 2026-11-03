package controller

import (
	"testing"

	"github.com/stretchr/testify/require"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestValidateLedgerVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		ledgerVersion   string
		operatorVersion string
		wantReason      string
		wantError       bool
	}{
		{name: "older release", ledgerVersion: "v3.0.0-rc.1", operatorVersion: "v3.0.0-rc.2", wantReason: "Compatible"},
		{name: "equal release", ledgerVersion: "3.0.0-rc.2", operatorVersion: "v3.0.0-rc.2", wantReason: "Compatible"},
		{name: "newer release", ledgerVersion: "v3.0.0-rc.3", operatorVersion: "v3.0.0-rc.2", wantReason: "LedgerVersionAheadOfOperator", wantError: true},
		{name: "stable after candidate", ledgerVersion: "v3.0.0", operatorVersion: "v3.0.0-rc.2", wantReason: "LedgerVersionAheadOfOperator", wantError: true},
		{name: "next candidate after stable", ledgerVersion: "v3.0.1-rc.1", operatorVersion: "v3.0.0", wantReason: "LedgerVersionAheadOfOperator", wantError: true},
		{name: "build metadata does not change precedence", ledgerVersion: "v3.0.0+ledger", operatorVersion: "v3.0.0+operator", wantReason: "Compatible"},
		{name: "opaque Ledger tag", ledgerVersion: "pr-123-abcdef0", operatorVersion: "v3.0.0", wantReason: "LedgerVersionUnknown", wantError: true},
		{name: "latest Ledger tag", ledgerVersion: "latest", operatorVersion: "v3.0.0", wantReason: "LedgerVersionUnknown", wantError: true},
		{name: "development tags", ledgerVersion: "e2e", operatorVersion: "dev", wantReason: "DevelopmentVersions"},
		{name: "unknown operator version", ledgerVersion: "v3.0.0", operatorVersion: "dev", wantReason: "OperatorVersionUnknown", wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ledger := &ledgerv1alpha1.Cluster{}
			ledger.Spec.Image.Tag = tt.ledgerVersion
			reason, err := validateLedgerVersion(ledger, tt.operatorVersion)
			require.Equal(t, tt.wantReason, reason)
			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
