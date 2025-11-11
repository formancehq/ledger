package clickhouse

import (
	"context"

	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type dsnProvider interface {
	GetDSN() pulumix.Output[string]
}

type componentFacade struct {
	cmp dsnProvider
}

func (e *componentFacade) GetConfig() pulumi.AnyOutput {
	return pulumix.Apply(e.cmp.GetDSN(), func(dsn string) any {
		return map[string]any{
			"dsn": dsn,
		}
	}).Untyped().(pulumi.AnyOutput)
}

func (b *componentFacade) GetDevBoxContainer(ctx context.Context) corev1.ContainerInput {
	return corev1.ContainerArgs{
		Name:  pulumi.String("clickhouse"),
		Image: pulumi.String("clickhouse:25.1"),
		Command: pulumi.StringArray{
			pulumi.String("sleep"),
		},
		Args: pulumi.StringArray{
			pulumi.String("infinity"),
		},
		Env: corev1.EnvVarArray{
			corev1.EnvVarArgs{
				Name: pulumi.String("CLICKHOUSE_URL"),
				Value: b.cmp.GetDSN().
					ToOutput(ctx).
					Untyped().(pulumi.StringOutput),
			},
		},
	}
}

func newComponentFacade(cmp dsnProvider) *componentFacade {
	return &componentFacade{
		cmp: cmp,
	}
}
