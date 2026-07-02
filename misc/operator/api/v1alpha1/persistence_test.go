package v1alpha1

import "testing"

// TestDeletionProtectionEnabled pins the opt-out default: an omitted field (nil)
// resolves to protected, matching the CRD default of true, so a LedgerService
// constructed in-process without API-server defaulting is protected unless it
// explicitly opts out.
func TestDeletionProtectionEnabled(t *testing.T) {
	t.Parallel()

	trueVal, falseVal := true, false
	tests := []struct {
		name  string
		field *bool
		want  bool
	}{
		{name: "unset defaults to protected", field: nil, want: true},
		{name: "explicit true", field: &trueVal, want: true},
		{name: "explicit false opts out", field: &falseVal, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := PersistenceSpec{DeletionProtection: tc.field}.DeletionProtectionEnabled()
			if got != tc.want {
				t.Fatalf("DeletionProtectionEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}
