package worker

import (
	"encoding/json"
	"fmt"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/api"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/storage"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/utils"
	appsv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apps/v1"
	batchv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/batch/v1"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type Args struct {
	TerminationGracePeriodSeconds pulumix.Input[*int]
	Connectors                    map[string]pulumi.Map
}

func (args *Args) SetDefaults() {
	if args.TerminationGracePeriodSeconds == nil {
		args.TerminationGracePeriodSeconds = pulumix.Val((*int)(nil))
	}
}

type Component struct {
	pulumi.ResourceState

	Deployment *appsv1.Deployment
	Service    *corev1.Service
}

type ComponentArgs struct {
	utils.CommonArgs
	Args
	Database *storage.Component
	API      *api.Component
}

func NewComponent(ctx *pulumi.Context, name string, args ComponentArgs, opts ...pulumi.ResourceOption) (*Component, error) {
	cmp := &Component{}
	err := ctx.RegisterComponentResource("Formance:Ledger:Worker", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	envVars := corev1.EnvVarArray{}
	envVars = append(envVars, corev1.EnvVarArgs{
		Name:  pulumi.String("DEBUG"),
		Value: utils.BoolToString(args.Debug).Untyped().(pulumi.StringOutput),
	})

	envVars = append(envVars, args.Database.GetEnvVars()...)
	if otel := args.Otel; otel != nil {
		envVars = append(envVars, args.Otel.GetEnvVars(ctx.Context())...)
	}

	cmp.Deployment, err = appsv1.NewDeployment(ctx, "ledger-worker", &appsv1.DeploymentArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
			Labels: pulumi.StringMap{
				"com.formance.stack/app": pulumi.String("ledger"),
			},
		},
		Spec: appsv1.DeploymentSpecArgs{
			Replicas: pulumi.Int(1),
			Selector: &metav1.LabelSelectorArgs{
				MatchLabels: pulumi.StringMap{
					"com.formance.stack/app": pulumi.String("ledger-worker"),
				},
			},
			Template: &corev1.PodTemplateSpecArgs{
				Metadata: &metav1.ObjectMetaArgs{
					Labels: pulumi.StringMap{
						"com.formance.stack/app": pulumi.String("ledger-worker"),
					},
				},
				Spec: corev1.PodSpecArgs{
					TerminationGracePeriodSeconds: args.TerminationGracePeriodSeconds.ToOutput(ctx.Context()).Untyped().(pulumi.IntPtrOutput),
					Containers: corev1.ContainerArray{
						corev1.ContainerArgs{
							Name:            pulumi.String("worker"),
							Image:           utils.GetImage(args.Tag),
							ImagePullPolicy: args.ImagePullPolicy.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
							Args: pulumi.StringArray{
								pulumi.String("worker"),
							},
							Env: envVars,
						},
					},
				},
			},
		},
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, fmt.Errorf("creating deployment: %w", err)
	}

	for name, config := range args.Connectors {
		_, err := batchv1.NewJob(ctx, "initialize-connector-"+name, &batchv1.JobArgs{
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
										pulumi.All(config).ApplyT(func(m []interface{}) (string, error) {
											data, err := json.Marshal(map[string]any{
												"driver": name,
												"config": m[0],
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
