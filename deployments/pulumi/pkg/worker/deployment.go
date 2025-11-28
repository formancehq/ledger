package worker

import (
	appsv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apps/v1"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"

	"github.com/formancehq/ledger/deployments/pulumi/pkg/utils"
)

func createDeployment(ctx *pulumi.Context, args ComponentArgs, resourceOptions ...pulumi.ResourceOption) (*appsv1.Deployment, error) {
	envVars := corev1.EnvVarArray{}
	envVars = append(envVars, corev1.EnvVarArgs{
		Name:  pulumi.String("DEBUG"),
		Value: utils.BoolToString(args.Debug).Untyped().(pulumi.StringOutput),
	})

	envVars = append(envVars, args.Database.GetEnvVars()...)
	if otel := args.Monitoring; otel != nil {
		envVars = append(envVars, args.Monitoring.GetEnvVars(ctx)...)
	}

	return appsv1.NewDeployment(ctx, "ledger-worker", &appsv1.DeploymentArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
			Labels: pulumi.StringMap{
				"com.formance.stack/app": pulumi.String("ledger-worker"),
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
							Image:           utils.GetMainImage(args.ImageConfiguration),
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
	}, resourceOptions...)
}
