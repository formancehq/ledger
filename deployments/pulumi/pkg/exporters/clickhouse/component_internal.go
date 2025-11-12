package clickhouse

import (
	"context"
	"errors"
	"fmt"

	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	helm "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v4"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/internals"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"

	"github.com/formancehq/ledger/deployments/pulumi/pkg/common"
)

type internalComponent struct {
	pulumi.ResourceState

	DSN     pulumix.Output[string]
	Chart   *helm.Chart
	Service *corev1.Service
}

func (e *internalComponent) GetDSN() pulumix.Output[string] {
	return pulumix.Apply2(
		e.Service.Metadata.Name().Elem(),
		e.Service.Metadata.Namespace().Elem(),
		func(name string, namespace string) string {
			return fmt.Sprintf(
				"clickhouse://default:password@%s.%s.svc.cluster.local:%d",
				name,
				namespace,
				9000,
			)
		},
	)
}

type internalComponentArgs struct {
	common.CommonArgs
	Config          pulumi.MapInput
	RetainsOnDelete bool
}

func newInternalComponent(ctx *pulumi.Context, name string, args internalComponentArgs, opts ...pulumi.ResourceOption) (*internalComponent, error) {
	cmp := &internalComponent{}
	err := ctx.RegisterComponentResource("Formance:Ledger:Clickhouse:Internal", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	chartOptions := []pulumi.ResourceOption{
		pulumi.Parent(cmp),
	}
	if args.RetainsOnDelete {
		chartOptions = append(chartOptions,
			pulumi.RetainOnDelete(true),
			// see https://github.com/pulumi/pulumi-kubernetes/issues/3065
			pulumi.Transforms([]pulumi.ResourceTransform{
				func(ctx context.Context, args *pulumi.ResourceTransformArgs) *pulumi.ResourceTransformResult {
					args.Opts.RetainOnDelete = true
					return &pulumi.ResourceTransformResult{
						Props: args.Props,
						Opts:  args.Opts,
					}
				},
			}),
		)
	}

	cmp.Chart, err = helm.NewChart(ctx, "clickhouse", &helm.ChartArgs{
		Chart:     pulumi.String("oci://registry-1.docker.io/bitnamicharts/clickhouse"),
		Version:   pulumi.String("8.0.6"),
		Name:      pulumi.String("clickhouse"),
		Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
		Values: pulumix.Apply(args.Config.ToMapOutput(), func(values map[string]any) map[string]any {
			// Add sane default for development
			if values == nil {
				values = map[string]any{}
			}
			if values["replicaCount"] == nil {
				values["replicaCount"] = 1
			}
			if values["shards"] == nil {
				values["shards"] = 1
			}
			if values["zookeeper"] == nil {
				values["zookeeper"] = map[string]any{
					"enabled": false,
				}
			}
			if values["auth"] == nil {
				values["auth"] = map[string]any{
					"password": "password",
				}
			}
			return values
		}).Untyped().(pulumi.MapOutput),
	}, chartOptions...)
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
					// find the first service with a cluster ip address
					if clusterIP == "None" {
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
			return service, nil
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

var _ dsnProvider = (*internalComponent)(nil)
