package connectors

import (
	"context"
	"fmt"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/common"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"reflect"
)

type ConnectorComponent interface {
	GetConfig() pulumi.AnyOutput
	GetDevBoxContainer(context context.Context) corev1.ContainerInput
}

type Factory[CONFIG any] interface {
	Name() string
	Setup(ctx *pulumi.Context, args common.CommonArgs, config CONFIG, options []pulumi.ResourceOption) (ConnectorComponent, error)
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
