package exporters

import (
	"context"
	"fmt"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/common"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"reflect"
)

type ExporterComponent interface {
	GetConfig() pulumi.AnyOutput
	GetDevBoxContainer(context context.Context) corev1.ContainerInput
}

type Factory[CONFIG any] interface {
	Name() string
	Setup(ctx *pulumi.Context, args common.CommonArgs, config CONFIG, options []pulumi.ResourceOption) (ExporterComponent, error)
}

var exporterFactories = map[string]any{}

func RegisterExporterFactory[CONFIG any](exporter Factory[CONFIG]) {
	exporterFactories[exporter.Name()] = exporter
}

func GetExporterConfig(name string) (any, error) {
	exporter, ok := exporterFactories[name]
	if !ok {
		return nil, fmt.Errorf("exporter %s not found", name)
	}

	m, _ := reflect.TypeOf(exporter).MethodByName("Setup")
	cfg := m.Type.In(3)

	return reflect.New(cfg).Interface(), nil
}
