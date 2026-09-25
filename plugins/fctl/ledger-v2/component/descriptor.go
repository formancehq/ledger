// Package component assembles the immutable Ledger-v2 portable descriptor.
package component

import (
	pb "github.com/formancehq/fctl-v2-poc/pkg/plugin/protocol/componentbridgev1alpha1"
	ledgerv2 "github.com/formancehq/ledger/plugins/fctl/ledger-v2"
)

func Descriptor() pb.Descriptor {
	plugin := ledgerv2.Plugin{}
	return pb.Descriptor{
		Metadata:               plugin.Metadata(),
		Commands:               plugin.Commands(),
		DocumentationResources: plugin.DocumentationResources(),
	}
}
