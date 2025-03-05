package devbox

import (
	"fmt"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/storage"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/utils"
	appsv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apps/v1"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

type Component struct {
	pulumi.ResourceState
	Deployment *appsv1.Deployment
}

type ComponentArgs struct {
	utils.CommonArgs
	Storage *storage.Component
}

func NewComponent(ctx *pulumi.Context, name string, args ComponentArgs, opts ...pulumi.ResourceOption) (*Component, error) {
	cmp := &Component{}
	err := ctx.RegisterComponentResource("Formance:Ledger:DevBox", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	cmp.Deployment, err = appsv1.NewDeployment(ctx, "ledger-devbox", &appsv1.DeploymentArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
			Labels: pulumi.StringMap{
				"com.formance.stack/app": pulumi.String("ledger-devbox"),
			},
		},
		Spec: appsv1.DeploymentSpecArgs{
			Selector: &metav1.LabelSelectorArgs{
				MatchLabels: pulumi.StringMap{
					"com.formance.stack/app": pulumi.String("ledger-devbox"),
				},
			},
			Template: &corev1.PodTemplateSpecArgs{
				Metadata: &metav1.ObjectMetaArgs{
					Labels: pulumi.StringMap{
						"com.formance.stack/app": pulumi.String("ledger-devbox"),
					},
				},
				Spec: corev1.PodSpecArgs{
					TerminationGracePeriodSeconds: pulumi.IntPtr(0),
					Containers: corev1.ContainerArray{
						corev1.ContainerArgs{
							Name:            pulumi.String("ledger"),
							Image:           pulumi.String("alpine:3.21"),
							ImagePullPolicy: pulumi.String("IfNotPresent"),
							Args: pulumi.StringArray{
								pulumi.String("sh"),
								pulumi.String("-c"),
								pulumi.String(`
									#!/bin/sh
									
									apk update
									apk add postgresql-client httpie bash

									sleep infinity
								`),
							},
							Env: corev1.EnvVarArray{
								corev1.EnvVarArgs{
									Name:  pulumi.String("POSTGRES_SERVICE_NAME"),
									Value: args.Storage.Service.Metadata.Name(),
								},
								corev1.EnvVarArgs{
									Name: pulumi.String("POSTGRES_USERNAME"),
									ValueFrom: corev1.EnvVarSourceArgs{
										SecretKeyRef: &corev1.SecretKeySelectorArgs{
											Key:  pulumi.String("username"),
											Name: args.Storage.Credentials.Metadata.Name(),
										},
									},
								},
								corev1.EnvVarArgs{
									Name: pulumi.String("POSTGRES_PASSWORD"),
									ValueFrom: corev1.EnvVarSourceArgs{
										SecretKeyRef: &corev1.SecretKeySelectorArgs{
											Key:  pulumi.String("password"),
											Name: args.Storage.Credentials.Metadata.Name(),
										},
									},
								},
								corev1.EnvVarArgs{
									Name:  pulumi.String("PGPASSWORD"),
									Value: pulumi.String("$(POSTGRES_PASSWORD)"),
								},
							},
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
