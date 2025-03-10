package connectors

import (
	"fmt"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/utils"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"reflect"
)

type ConnectorComponent interface {
	pulumi.ComponentResource
	GetConfig() pulumi.AnyOutput
}

type Factory[CONFIG any] interface {
	Name() string
	Setup(ctx *pulumi.Context, args utils.CommonArgs, config CONFIG, options []pulumi.ResourceOption) (ConnectorComponent, error)
}

var connectorFactories = map[string]any{}

func RegisterConnectorFactory[CONFIG any](connector Factory[CONFIG]) {
	connectorFactories[connector.Name()] = connector
}

func GetConnectorConfig(name string) (any, error) {
	connector, ok := connectorFactories[name]
	if !ok {
		return nil, fmt.Errorf("connector %s not found", name)
	}

	m, _ := reflect.TypeOf(connector).MethodByName("Setup")
	cfg := m.Type.In(3)

	return reflect.New(cfg).Interface(), nil
}
