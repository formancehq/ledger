package ledgerv2

import (
	"context"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

const (
	Name    = "ledger-v2"
	Version = "0.1.0"
)

type Plugin struct{}

var _ sdk.Plugin = Plugin{}

func Metadata() sdk.Metadata { return (Plugin{}).Metadata() }

func (Plugin) Metadata() sdk.Metadata {
	return sdk.Metadata{
		Name:    Name,
		Version: Version,
		Facets: []sdk.Facet{{
			Kind:                     sdk.FacetCommandProvider,
			ProtocolVersion:          sdk.CurrentCommandProviderFacetProtocolVersion,
			RequiredHostCapabilities: sdk.RequiredHostCapabilitiesForCommands(Commands()),
		}},
	}
}

func (Plugin) Commands() []sdk.Command { return Commands() }

func (Plugin) DocumentationResources() []sdk.DocumentationResource { return nil }

func (Plugin) Execute(ctx context.Context, request sdk.ExecuteRequest, host sdk.Host) error {
	return executeGeneratedV2(ctx, request, host)
}
