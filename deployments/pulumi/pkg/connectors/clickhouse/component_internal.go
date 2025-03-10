package clickhouse

import (
	"errors"
	"fmt"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/connectors"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/utils"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	helm "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v4"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/internals"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type internalComponent struct {
	pulumi.ResourceState

	DSN     pulumix.Output[string]
	Chart   *helm.Chart
	Service *corev1.Service
}

func (e *internalComponent) GetConfig() pulumi.AnyOutput {
	return pulumi.Any(map[string]any{
		"dsn": pulumi.Sprintf(
			"clickhouse://default:password@%s.%s.svc.cluster.local:%d",
			e.Service.Metadata.Name().Elem(),
			e.Service.Metadata.Namespace().Elem(),
			9000,
		),
	})
}

type internalComponentArgs struct {
	utils.CommonArgs
}

func newInternalComponent(ctx *pulumi.Context, name string, args internalComponentArgs, opts ...pulumi.ResourceOption) (*internalComponent, error) {
	cmp := &internalComponent{}
	err := ctx.RegisterComponentResource("Formance:Ledger:Clickhouse:Internal", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	cmp.Chart, err = helm.NewChart(ctx, "clickhouse", &helm.ChartArgs{
		Chart:     pulumi.String("oci://registry-1.docker.io/bitnamicharts/clickhouse"),
		Version:   pulumi.String("8.0.5"),
		Name:      pulumi.String("clickhouse"),
		Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
		Values: pulumi.Map{
			"replicaCount": pulumi.Int(1),
			"shards":       pulumi.Int(1),
			"zookeeper": pulumi.Map{
				"enabled": pulumi.Bool(false),
			},
			"resources": pulumi.Map{
				"requests": pulumi.Map{
					"cpu":    pulumi.String("2"),
					"memory": pulumi.String("1Gi"),
				},
				"limits": pulumi.Map{
					"cpu":    pulumi.String("2"),
					"memory": pulumi.String("2Gi"),
				},
			},
			"auth": pulumi.Map{
				//"username": pulumi.String("admin"),
				"password": pulumi.String("password"),
			},
		},
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, err
	}

	ret, err := internals.UnsafeAwaitOutput(ctx.Context(), pulumix.ApplyErr(cmp.Chart.Resources, func(resources []any) (*corev1.Service, error) {
		for _, resource := range resources {
			service, ok := resource.(*corev1.Service)
			if !ok {
				continue
			}
			ret, err := internals.UnsafeAwaitOutput(ctx.Context(), pulumix.Apply2(
				service.Spec.Type().Elem(),
				service.Spec.ClusterIP().Elem(),
				func(serviceType, clusterIP string) *corev1.Service {
					// select not headless service
					if serviceType != "ClusterIP" || clusterIP == "None" {
						return nil
					}
					return service
				},
			))
			if err != nil {
				return nil, err
			}
			if ret.Value != nil {
				return ret.Value.(*corev1.Service), nil
			}
		}
		return nil, errors.New("not found")
	}))
	if err != nil {
		return nil, err
	}
	cmp.Service = ret.Value.(*corev1.Service)

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}

var _ connectors.ConnectorComponent = (*internalComponent)(nil)