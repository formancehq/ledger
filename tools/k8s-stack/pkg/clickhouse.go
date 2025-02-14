package ledger_stack

import (
	"fmt"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type ClickHouseComponentArgs struct {
	Namespace pulumix.Input[string]
}

type ClickHouseComponent struct {
	pulumi.ResourceState

	Username pulumix.Output[string]
	Password pulumix.Output[string]
	Host     pulumix.Output[string]
	Port     pulumix.Output[int]
}

func NewClickHouseComponent(ctx *pulumi.Context, name string, args *ClickHouseComponentArgs, opts ...pulumi.ResourceOption) (*ClickHouseComponent, error) {
	cmp := &ClickHouseComponent{}
	err := ctx.RegisterComponentResource("Formance:Ledger:Testing:ClickHouse", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	username := pulumix.Val("root")
	password := pulumix.Val("password")

	release, err := helm.NewRelease(ctx, "clickhouse", &helm.ReleaseArgs{
		Chart:     pulumi.String("oci://registry-1.docker.io/bitnamicharts/clickhouse"),
		Version:   pulumi.String("8.0.0"),
		Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
		Values: pulumi.Map{
			"auth": pulumi.Map{
				"username": username,
				"password": password,
			},
		},
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, err
	}

	cmp.Username = username
	cmp.Password = password
	cmp.Host = pulumix.Apply2(
		release.Status.Name().ToOutput(ctx.Context()).Untyped().(pulumi.StringPtrOutput),
		release.Status.Namespace().ToOutput(ctx.Context()).Untyped().(pulumi.StringPtrOutput),
		func(name, namespace *string) string {
			return fmt.Sprintf("%s-headless.%s.svc.cluster.local", *name, *namespace)
		})
	cmp.Port = pulumix.Val(9000)

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}
