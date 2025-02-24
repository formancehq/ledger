package pulumi_ledger

import (
	batchv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/batch/v1"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

func newMigrationJob(ctx *pulumi.Context, cmp *Component, args *ComponentArgs) (*batchv1.Job, error) {
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
							Image:           image(args.Tag),
							ImagePullPolicy: args.ImagePullPolicy.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
							Env: corev1.EnvVarArray{
								corev1.EnvVarArgs{
									Name:  pulumi.String("POSTGRES_URI"),
									Value: args.Postgres.URI.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
								},
								corev1.EnvVarArgs{
									Name:  pulumi.String("DEBUG"),
									Value: boolToString(args.Debug).Untyped().(pulumi.StringOutput),
								},
							},
						},
					},
				},
			},
		},
	}, pulumi.Parent(cmp))
}