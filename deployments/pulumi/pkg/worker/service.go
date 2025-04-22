package worker

import (
	appsv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apps/v1"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

func createService(ctx *pulumi.Context, args ComponentArgs, deployment *appsv1.Deployment, opts ...pulumi.ResourceOption) (*corev1.Service, error) {
	return corev1.NewService(ctx, "ledger-worker", &corev1.ServiceArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
		},
		Spec: &corev1.ServiceSpecArgs{
			Selector: deployment.Spec.Selector().MatchLabels(),
			Type:     pulumi.String("ClusterIP"),
			Ports: corev1.ServicePortArray{
				corev1.ServicePortArgs{
					Port:       pulumi.Int(8081),
					TargetPort: pulumi.Int(8081),
					Protocol:   pulumi.String("TCP"),
					Name:       pulumi.String("grpc"),
				},
			},
		},
	}, opts...)
}
