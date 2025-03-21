package worker

import (
	"fmt"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/api"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/common"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/storage"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/utils"
	appsv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apps/v1"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type Args struct {
	TerminationGracePeriodSeconds pulumix.Input[*int]
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
	common.CommonArgs
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
	if otel := args.Monitoring; otel != nil {
		envVars = append(envVars, args.Monitoring.GetEnvVars(ctx)...)
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
							Image:           utils.GetMainImage(args.Tag),
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

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}
