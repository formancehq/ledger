//go:build fctl_component_guest

package export_formance_fctl_plugin_lifecycle

import (
	"testing"

	pb "github.com/formancehq/fctl-v2-poc/pkg/plugin/protocol/componentbridgev1alpha1"
	ledgerv3 "github.com/formancehq/ledger/v3/plugins/fctl/ledger-v3"
)

func TestDescribeExportsAllLedgerV3Commands(t *testing.T) {
	descriptor, err := pb.DecodeDescriptorEnvelope(Describe())
	if err != nil {
		t.Fatalf("decode descriptor: %v", err)
	}
	if descriptor.Metadata.Name != ledgerv3.Name || descriptor.Metadata.Version != ledgerv3.Version {
		t.Fatalf("descriptor identity = %q/%q", descriptor.Metadata.Name, descriptor.Metadata.Version)
	}
	if len(descriptor.Commands) != 48 {
		t.Fatalf("descriptor command count = %d, want 48", len(descriptor.Commands))
	}
}

func TestLifecycleExportsRejectInvalidOrUnknownExecutionsWithoutPanicking(t *testing.T) {
	if events := Start("invalid-start", []byte("not an execution envelope")); len(events) == 0 {
		t.Fatal("Start returned no terminal event for an invalid envelope")
	}
	if events := Resume("unknown-resume", nil); events != nil {
		t.Fatalf("Resume returned events for an unknown execution: %v", events)
	}
	if events := Cancel("unknown-cancel"); events != nil {
		t.Fatalf("Cancel returned events for an unknown execution: %v", events)
	}
	Close("unknown-close")
}
