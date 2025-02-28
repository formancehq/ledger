package storage

import (
	"github.com/formancehq/ledger/deployments/pulumi/pkg/utils"
	batchv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/batch/v1"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

type migrationArgs struct {
	utils.CommonArgs
	component *Component
}

func runMigrateJob(ctx *pulumi.Context, args migrationArgs, opts ...pulumi.ResourceOption) (*batchv1.Job, error) {
	envVars := corev1.EnvVarArray{
		corev1.EnvVarArgs{
			Name:  pulumi.String("DEBUG"),
			Value: utils.BoolToString(args.Debug).Untyped().(pulumi.StringOutput),
		},
	}
	envVars = append(envVars, args.component.GetEnvVars()...)

	return batchv1.NewJob(ctx, "migrate", &batchv1.JobArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
		},
		Spec: batchv1.JobSpecArgs{
			Template: corev1.PodTemplateSpecArgs{
				Spec: corev1.PodSpecArgs{
					RestartPolicy: pulumi.String("OnFailure"),
					Containers: corev1.ContainerArray{
						corev1.ContainerArgs{
							Name: pulumi.String("migrate"),
							Args: pulumi.StringArray{
								pulumi.String("migrate"),
							},
							Image:           utils.GetImage(args.Tag),
							ImagePullPolicy: args.ImagePullPolicy.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
							Env:             envVars,
						},
					},
				},
			},
		},
	}, append(opts, pulumi.DeleteBeforeReplace(true))...)
}
