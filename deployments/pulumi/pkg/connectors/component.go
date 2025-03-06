package connectors

import (
	"fmt"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/common"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"reflect"
)

type ConnectorArgs struct {
	Driver string
	Config any
}

type Args struct {
	Connectors map[string]ConnectorArgs
}

type Connector struct {
	Driver    string
	Component ConnectorComponent
}

type Component struct {
	pulumi.ResourceState

	Connectors map[string]Connector
}

type ComponentArgs struct {
	common.CommonArgs
	Args
}

func NewComponent(ctx *pulumi.Context, name string, args ComponentArgs, opts ...pulumi.ResourceOption) (*Component, error) {
	cmp := &Component{
		Connectors: map[string]Connector{},
	}
	err := ctx.RegisterComponentResource("Formance:Ledger:Connectors", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	for id, connector := range args.Connectors {
		factory, ok := connectorFactories[connector.Driver]
		if !ok {
			return nil, fmt.Errorf("connector %s not found", name)
		}

		m := reflect.ValueOf(factory).
			MethodByName("Setup").
			Call([]reflect.Value{
				reflect.ValueOf(ctx),
				reflect.ValueOf(args.CommonArgs),
				reflect.ValueOf(connector.Config),
				reflect.ValueOf([]pulumi.ResourceOption{
					pulumi.Parent(cmp),
				}),
			})
		if !m[1].IsZero() {
			return nil, m[1].Interface().(error)
		}

		cmp.Connectors[id] = Connector{
			Driver:    connector.Driver,
			Component: m[0].Interface().(ConnectorComponent),
		}
	}

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}
