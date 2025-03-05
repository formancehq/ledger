package api

import (
	"github.com/formancehq/ledger/deployments/pulumi/pkg/utils"
	v1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apps/v1"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

type createServiceArgs struct {
	utils.CommonArgs
	Deployment *v1.Deployment
}

func createService(ctx *pulumi.Context, args createServiceArgs, opts ...pulumi.ResourceOption) (*corev1.Service, error) {
	return corev1.NewService(ctx, "ledger", &corev1.ServiceArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
		},
		Spec: &corev1.ServiceSpecArgs{
			Selector: args.Deployment.Spec.Selector().MatchLabels(),
			Type:     pulumi.String("ClusterIP"),
			Ports: corev1.ServicePortArray{
				corev1.ServicePortArgs{
					Port:       pulumi.Int(8080),
					TargetPort: pulumi.Int(8080),
					Protocol:   pulumi.String("TCP"),
					Name:       pulumi.String("http"),
				},
			},
		},
	}, opts...)
}
