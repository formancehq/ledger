package api

import (
	"github.com/formancehq/ledger/deployments/pulumi/pkg/common"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	networkingv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/networking/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type IngressArgs struct {
	Host    pulumix.Input[string]
	Secret  pulumix.Input[*string]
	Service *corev1.Service
}

type createIngressArgs struct {
	common.CommonArgs
	IngressArgs
}

func createIngress(ctx *pulumi.Context, args createIngressArgs, opts ...pulumi.ResourceOption) (*networkingv1.Ingress, error) {
	return networkingv1.NewIngress(ctx, "ledger", &networkingv1.IngressArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
		},
		Spec: &networkingv1.IngressSpecArgs{
			Rules: networkingv1.IngressRuleArray{
				networkingv1.IngressRuleArgs{
					Host: args.Host.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
					Http: networkingv1.HTTPIngressRuleValueArgs{
						Paths: networkingv1.HTTPIngressPathArray{
							networkingv1.HTTPIngressPathArgs{
								Backend: networkingv1.IngressBackendArgs{
									Service: &networkingv1.IngressServiceBackendArgs{
										Name: args.Service.Metadata.Name().ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
										Port: networkingv1.ServiceBackendPortArgs{
											Name: pulumi.String("http"),
										},
									},
								},
								Path:     pulumi.String("/"),
								PathType: pulumi.String("Prefix"),
							},
						},
					},
				},
			},
			Tls: pulumix.Apply(args.Secret, func(secret *string) networkingv1.IngressTLSArrayInput {
				if secret == nil || *secret == "" {
					return nil
				}

				return networkingv1.IngressTLSArray{
					networkingv1.IngressTLSArgs{
						Hosts: pulumi.StringArray{
							args.Host.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
						},
						SecretName: pulumi.String(*secret),
					},
				}
			}).Untyped().(pulumi.AnyOutput).ApplyT(func(v any) networkingv1.IngressTLSArrayInput {
				if v == nil {
					return nil
				}
				return v.(networkingv1.IngressTLSArrayInput)
			}).(networkingv1.IngressTLSArrayInput),
		},
	}, opts...)
}
