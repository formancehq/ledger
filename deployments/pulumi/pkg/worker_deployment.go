package pulumi_ledger

import (
	appsv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apps/v1"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type WorkerArgs struct {
	TerminationGracePeriodSeconds pulumix.Input[*int]
}

func (args *WorkerArgs) setDefaults() {
	if args.TerminationGracePeriodSeconds == nil {
		args.TerminationGracePeriodSeconds = pulumix.Val((*int)(nil))
	}
}

func createWorkerDeployment(ctx *pulumi.Context, cmp *Component, args *ComponentArgs) (*appsv1.Deployment, error) {
	ledgerImage := pulumi.Sprintf("ghcr.io/formancehq/ledger:%s", args.Tag)

	envVars := corev1.EnvVarArray{}
	envVars = append(envVars, corev1.EnvVarArgs{
		Name:  pulumi.String("DEBUG"),
		Value: boolToString(args.Debug).Untyped().(pulumi.StringOutput),
	})

	envVars = append(envVars, args.Postgres.getEnvVars(ctx.Context())...)
	if otel := args.Otel; otel != nil {
		envVars = append(envVars, args.getOpenTelemetryEnvVars(ctx.Context())...)
	}

	return appsv1.NewDeployment(ctx, "ledger-worker", &appsv1.DeploymentArgs{
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
					TerminationGracePeriodSeconds: args.Worker.TerminationGracePeriodSeconds.ToOutput(ctx.Context()).Untyped().(pulumi.IntPtrOutput),
					Containers: corev1.ContainerArray{
						corev1.ContainerArgs{
							Name:            pulumi.String("worker"),
							Image:           ledgerImage,
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
}
