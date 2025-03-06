package clickhouse

import (
	"errors"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/common"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/connectors"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

type InstallConfiguration struct {
	Configuration   map[string]any `json:"configuration" yaml:"configuration"`
	RetainsOnDelete bool           `json:"retains-on-delete" yaml:"retains-on-delete"`
}

type Configuration struct {
	DSN     string                `json:"dsn" yaml:"dsn"`
	Install *InstallConfiguration `json:"install" yaml:"install"`
}

type Factory struct{}

func (f Factory) Name() string {
	return "clickhouse"
}

func (f Factory) Setup(ctx *pulumi.Context, args common.CommonArgs, config Configuration, options []pulumi.ResourceOption) (connectors.ConnectorComponent, error) {
	var (
		cmp dsnProvider
		err error
	)
	if config.DSN != "" {
		cmp, err = newExternalComponent(ctx, "clickhouse", externalComponentArgs{
			DSN: pulumi.String(config.DSN),
		}, options...)
	} else if config.Install != nil {
		cmp, err = newInternalComponent(ctx, "clickhouse", internalComponentArgs{
			CommonArgs:      args,
			Config:          pulumi.ToMap(config.Install.Configuration),
			RetainsOnDelete: config.Install.RetainsOnDelete,
		}, options...)
	} else {
		return nil, errors.New("either DSN or Install configuration must be provided")
	}
	if err != nil {
		return nil, err
	}

	return newComponentFacade(cmp), nil
}

var _ connectors.Factory[Configuration] = (*Factory)(nil)

func init() {
	connectors.RegisterConnectorFactory(Factory{})
}
