package pulumi_postgres

import (
	"fmt"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

type PostgresComponent struct {
	pulumi.ResourceState

	Service pulumi.StringOutput
}

type PostgresComponentArgs struct {
	Namespace pulumi.StringInput
}

func NewPostgresComponent(ctx *pulumi.Context, name string, args *PostgresComponentArgs, opts ...pulumi.ResourceOption) (*PostgresComponent, error) {
	cmp := &PostgresComponent{}
	err := ctx.RegisterComponentResource("Formance:Postgres:Deployment", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	var namespace pulumi.StringInput = pulumi.String("default")
	if args != nil {
		namespace = args.Namespace
	}

	rel, err := helm.NewRelease(ctx, "postgres", &helm.ReleaseArgs{
		Chart:     pulumi.String("oci://registry-1.docker.io/bitnamicharts/postgresql"),
		Version:   pulumi.String("16.1.1"),
		Namespace: namespace,
		Values: pulumi.Map(map[string]pulumi.Input{
			"auth": pulumi.Map{
				"postgresPassword": pulumi.String("postgres"),
				"database":         pulumi.String("ledger"),
			},
			"primary": pulumi.Map{
				"resources": pulumi.Map{
					"requests": pulumi.Map{
						"memory": pulumi.String("256Mi"),
						"cpu":    pulumi.String("256m"),
					},
				},
			},
		}),
		CreateNamespace: pulumi.BoolPtr(true),
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, fmt.Errorf("installing release")
	}

	cmp.Service = pulumi.Sprintf(
		"%s-postgresql.%s",
		rel.Status.Name().Elem(),
		rel.Status.Namespace().Elem(),
	)
	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{"service": cmp.Service}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}
