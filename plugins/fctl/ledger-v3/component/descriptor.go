// Package component assembles the immutable Ledger v3 portable descriptor.
package component

import (
	pb "github.com/formancehq/fctl-v2-poc/pkg/plugin/protocol/componentbridgev1alpha1"
	ledgerv3 "github.com/formancehq/ledger/v3/plugins/fctl/ledger-v3"
)

// Descriptor returns the command-only descriptor served by the portable
// Ledger v3 component.
func Descriptor() pb.Descriptor {
	plugin := ledgerv3.Plugin{}
	return pb.Descriptor{
		Metadata:               plugin.Metadata(),
		Commands:               plugin.Commands(),
		DocumentationResources: plugin.DocumentationResources(),
	}
}
