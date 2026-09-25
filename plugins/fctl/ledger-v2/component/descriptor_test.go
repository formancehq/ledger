package component

import (
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	portablecomponent "github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk/portable/component"
	ledgerv2 "github.com/formancehq/ledger/plugins/fctl/ledger-v2"
)

func TestDescriptorCreatesAReconstructibleLedgerV2Component(t *testing.T) {
	descriptor := Descriptor()
	if descriptor.Metadata.Name != "ledger-v2" || len(descriptor.Commands) != 22 {
		t.Fatalf("descriptor identity/commands = %q/%d", descriptor.Metadata.Name, len(descriptor.Commands))
	}
	if len(descriptor.AuthProviders) != 0 || len(descriptor.TargetProviders) != 0 || len(descriptor.SignerProviders) != 0 {
		t.Fatalf("descriptor unexpectedly owns privileged facets: %#v", descriptor)
	}
	if err := sdk.ValidateCatalogue(descriptor.Commands, descriptor.DocumentationResources); err != nil {
		t.Fatalf("descriptor catalogue invalid: %v", err)
	}
	if _, err := portablecomponent.NewCommand(ledgerv2.Plugin{}, descriptor); err != nil {
		t.Fatalf("NewCommand() error = %v", err)
	}
}
