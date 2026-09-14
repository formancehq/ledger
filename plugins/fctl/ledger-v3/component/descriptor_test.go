package component

import (
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	ledgerv3 "github.com/formancehq/ledger/v3/plugins/fctl/ledger-v3"
)

func TestDescriptorContainsOnlyTheLedgerV3CommandFacet(t *testing.T) {
	descriptor := Descriptor()
	if descriptor.Metadata.Name != ledgerv3.Name || descriptor.Metadata.Version != ledgerv3.Version {
		t.Fatalf("descriptor identity = %q/%q", descriptor.Metadata.Name, descriptor.Metadata.Version)
	}
	if len(descriptor.Commands) != 48 {
		t.Fatalf("descriptor command count = %d, want 48", len(descriptor.Commands))
	}
	if len(descriptor.AuthProviders) != 0 || len(descriptor.TargetProviders) != 0 || len(descriptor.SignerProviders) != 0 {
		t.Fatalf("descriptor unexpectedly owns privileged facets: %#v", descriptor)
	}
	if err := sdk.ValidateCatalogue(descriptor.Commands, descriptor.DocumentationResources); err != nil {
		t.Fatalf("descriptor catalogue invalid: %v", err)
	}
}
