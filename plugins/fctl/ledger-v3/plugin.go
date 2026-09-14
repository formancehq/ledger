// Package ledgerv3 is the fctl command-provider plugin for Ledger v3.
//
// It serves the 48 runnable product command paths of `ledgerctl` at ledger release/v3.0
// 9a6fa7d0 over the gRPC service ledger.BucketService, and nothing else. Host
// and local concerns, operator and storage commands, and the signing and
// event-sink control plane are all owned elsewhere; see README.md for the
// complete accounting of the current executable commands.
//
// The plugin never resolves an endpoint, a credential or a target: it issues
// typed requests through the host's generated-client surface and the host owns
// transport entirely.
package ledgerv3

import (
	"context"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

// Name and Version identify this plugin to the host. They are shared with no
// other plugin; ledger-v2 is a separate catalogue with a separate identity.
const (
	Name    = "ledger-v3"
	Version = "0.1.0"
)

// Plugin is the command-provider facet for Ledger v3. It holds no state: one
// execution's transient state lives entirely inside Execute.
type Plugin struct{}

var _ sdk.Plugin = Plugin{}

// Metadata declares the single command-provider facet and the exact host
// capabilities derived from the command descriptors. Ledger v3 uses the
// generated-client surface and three commands also consume host-owned input
// artifacts.
func (Plugin) Metadata() sdk.Metadata {
	commands := (Plugin{}).Commands()
	return sdk.Metadata{
		Name:    Name,
		Version: Version,
		Facets: []sdk.Facet{{
			Kind:                     sdk.FacetCommandProvider,
			ProtocolVersion:          sdk.CurrentCommandProviderFacetProtocolVersion,
			RequiredHostCapabilities: sdk.RequiredHostCapabilitiesForCommands(commands),
		}},
	}
}

// Commands materialises the frozen catalogue.
func (Plugin) Commands() []sdk.Command {
	specs := catalogue()
	commands := make([]sdk.Command, 0, len(specs))
	for _, item := range specs {
		commands = append(commands, item.command())
	}
	return commands
}

// DocumentationResources contributes no bundled or linked documentation yet.
// Returning an explicit empty set keeps the catalogue's documentation
// references trivially consistent.
func (Plugin) DocumentationResources() []sdk.DocumentationResource { return nil }

// Execute runs one command through the explicitly versioned Ledger v3 adapter.
func (p Plugin) Execute(ctx context.Context, request sdk.ExecuteRequest, host sdk.Host) error {
	return executeV3(ctx, request, host)
}
