package api

import (
	"context"
	"fmt"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/common"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/storage"
	appsv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apps/v1"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

type Component struct {
	pulumi.ResourceState

	Deployment *appsv1.Deployment
	Service    *corev1.Service
}

func (cmp *Component) GetDevBoxContainer(_ context.Context) corev1.ContainerInput {
	return corev1.ContainerArgs{
		Name:  pulumi.String("api"),
		Image: pulumi.String("alpine/httpie:3.2.4"),
		Command: pulumi.StringArray{
			pulumi.String("sleep"),
		},
		Args: pulumi.StringArray{
			pulumi.String("infinity"),
		},
		Env: corev1.EnvVarArray{
			corev1.EnvVarArgs{
				Name: pulumi.String("API_URL"),
				Value: pulumi.Sprintf("http://%s.%s.svc.cluster.local:8080",
					cmp.Service.Metadata.Name().Elem(),
					cmp.Service.Metadata.Namespace().Elem(),
				),
			},
		},
	}
}

type ComponentArgs struct {
	common.CommonArgs
	Args
	Storage *storage.Component
	Ingress *IngressArgs
}

func NewComponent(ctx *pulumi.Context, name string, args ComponentArgs, opts ...pulumi.ResourceOption) (*Component, error) {
	cmp := &Component{}
	err := ctx.RegisterComponentResource("Formance:Ledger:API", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	cmp.Deployment, err = createDeployment(ctx, createDeploymentArgs{
		CommonArgs: args.CommonArgs,
		Args:       args.Args,
		Database:   args.Storage,
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, fmt.Errorf("creating deployment: %w", err)
	}

	cmp.Service, err = createService(ctx, createServiceArgs{
		CommonArgs: args.CommonArgs,
		Deployment: cmp.Deployment,
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, fmt.Errorf("creating service: %w", err)
	}

	if args.Ingress != nil {
		if _, err := createIngress(ctx, createIngressArgs{
			CommonArgs: args.CommonArgs,
			IngressArgs: IngressArgs{
				Host:    args.Ingress.Host,
				Secret:  args.Ingress.Secret,
				Service: cmp.Service,
			},
		}, pulumi.Parent(cmp),
		); err != nil {
			return nil, err
		}
	}

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}
