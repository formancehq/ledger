package clickhouse

import (
	"github.com/formancehq/ledger/deployments/pulumi/pkg/connectors"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/utils"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/internals"
)

type Configuration struct {
	DSN     pulumi.String
	Install pulumi.Bool
}

type Factory struct {}

func (f Factory) Name() string {
	return "clickhouse"
}

func (f Factory) Setup(ctx *pulumi.Context, args utils.CommonArgs, config Configuration, options []pulumi.ResourceOption) (connectors.ConnectorComponent, error) {
	dsn, err := internals.UnsafeAwaitOutput(ctx.Context(), config.DSN.ToOutput(ctx.Context()))
	if err != nil {
		return nil, err
	}

	if dsn.Value != nil && dsn.Value.(string) != "" {
		return newExternalComponent(ctx, "clickhouse", externalComponentArgs{
			DSN:        config.DSN,
		}, options...)
	}

	return newInternalComponent(ctx, "clickhouse", internalComponentArgs{
		CommonArgs: args,
	}, options...)
}

var _ connectors.Factory[Configuration] = (*Factory)(nil)

func init() {
	connectors.RegisterConnectorFactory(Factory{})
}