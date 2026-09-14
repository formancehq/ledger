//go:build fctl_component_guest

package json

import "testing"

func TestComponentGuestBackendUsesStandardLibrary(t *testing.T) {
	if backendName != "stdlib" {
		t.Fatalf("backend = %q, want stdlib", backendName)
	}
}
