//go:build !fctl_component_guest

package json

import "testing"

func TestNativeBackendRemainsSonic(t *testing.T) {
	if backendName != "sonic" {
		t.Fatalf("backend = %q, want sonic", backendName)
	}
}
