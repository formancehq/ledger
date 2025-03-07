package initialize

import (
	"encoding/json"
	"fmt"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/api"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/connectors"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/utils"
	batchv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/batch/v1"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type Connector struct {
	ID        string
	Driver    string
	connectors.Component
}

type Component struct {
	pulumi.ResourceState
}

type ComponentArgs struct {
	utils.CommonArgs
	Connectors                    *connectors.Component
	API *api.Component
}

func NewComponent(ctx *pulumi.Context, name string, args ComponentArgs, opts ...pulumi.ResourceOption) (*Component, error) {
	cmp := &Component{}
	err := ctx.RegisterComponentResource("Formance:Ledger:Initialize", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	for id, connector := range args.Connectors.Connectors {
		_, err := batchv1.NewJob(ctx, "initialize-connector-"+id, &batchv1.JobArgs{
			Metadata: metav1.ObjectMetaArgs{
				Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
			},
			Spec: batchv1.JobSpecArgs{
				Template: corev1.PodTemplateSpecArgs{
					Spec: corev1.PodSpecArgs{
						RestartPolicy: pulumi.String("Never"),
						Containers: corev1.ContainerArray{
							corev1.ContainerArgs{
								Name: pulumi.String("generator"),
								Args: pulumi.StringArray{
									pulumi.String("sh"),
									pulumi.String("-c"),
									pulumi.Sprintf(`
										curl -X POST \
											-H "Content-Type: application/json" \
											-d '%s' \
											--fail \
											http://%s.%s.svc.cluster.local:%d/v2/_system/connectors
									`,
										pulumix.ApplyErr(connector.Component.GetConfig(), func(cfg any) (string, error) {
											data, err := json.Marshal(map[string]any{
												"driver": connector.Driver,
												"config": cfg,
											})
											if err != nil {
												return "", err
											}

											return string(data), nil
										}),
										args.API.Service.Metadata.Name().Elem(),
										args.API.Service.Metadata.Namespace().Elem(),
										args.API.Service.Spec.Ports().Index(pulumi.Int(0)).Port(),
									),
								},
								Image: pulumi.String("alpine/curl"),
							},
						},
					},
				},
			},
		}, pulumi.Parent(cmp), pulumi.DeleteBeforeReplace(true))
		if err != nil {
			return nil, err
		}
	}

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}
