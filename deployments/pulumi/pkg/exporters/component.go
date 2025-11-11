package exporters

import (
	"fmt"
	"reflect"

	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"

	"github.com/formancehq/ledger/deployments/pulumi/pkg/common"
)

type ExporterArgs struct {
	Driver string
	Config any
}

type Args struct {
	Exporters map[string]ExporterArgs
}

type Exporter struct {
	Driver    string
	Component ExporterComponent
}

type Component struct {
	pulumi.ResourceState

	Exporters map[string]Exporter
}

type ComponentArgs struct {
	common.CommonArgs
	Args
}

func NewComponent(ctx *pulumi.Context, name string, args ComponentArgs, opts ...pulumi.ResourceOption) (*Component, error) {
	cmp := &Component{
		Exporters: map[string]Exporter{},
	}
	err := ctx.RegisterComponentResource("Formance:Ledger:Exporters", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	for id, exporter := range args.Exporters {
		factory, ok := exporterFactories[exporter.Driver]
		if !ok {
			return nil, fmt.Errorf("exporter %s not found", name)
		}

		m := reflect.ValueOf(factory).
			MethodByName("Setup").
			Call([]reflect.Value{
				reflect.ValueOf(ctx),
				reflect.ValueOf(args.CommonArgs),
				reflect.ValueOf(exporter.Config),
				reflect.ValueOf([]pulumi.ResourceOption{
					pulumi.Parent(cmp),
				}),
			})
		if !m[1].IsZero() {
			return nil, m[1].Interface().(error)
		}

		cmp.Exporters[id] = Exporter{
			Driver:    exporter.Driver,
			Component: m[0].Interface().(ExporterComponent),
		}
	}

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}
