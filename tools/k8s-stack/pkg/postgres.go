package ledger_stack

import (
	"fmt"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type PostgresComponent struct {
	pulumi.ResourceState

	Username pulumix.Output[string]
	Password pulumix.Output[string]
	Host     pulumix.Output[string]
	Port     pulumix.Output[int]
}

type PostgresComponentArgs struct {
	Namespace pulumix.Input[string]
}

func NewPostgresComponent(ctx *pulumi.Context, name string, args *PostgresComponentArgs, opts ...pulumi.ResourceOption) (*PostgresComponent, error) {
	cmp := &PostgresComponent{}
	err := ctx.RegisterComponentResource("Formance:Ledger:Testing:Postgres", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	username := pulumix.Val("root")
	password := pulumix.Val("password")

	_, err = helm.NewChart(ctx, "postgres", helm.ChartArgs{
		Chart:     pulumi.String("oci://registry-1.docker.io/bitnamicharts/postgresql"),
		Version:   pulumi.String("16.4.7"),
		Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
		Values: pulumi.Map{
			"global": pulumi.Map{
				"postgresql": pulumi.Map{
					"auth": pulumi.Map{
						"username": username,
						"password": password,
					},
				},
			},
		},
	})
	if err != nil {
		return nil, err
	}

	cmp.Username = username
	cmp.Password = password
	// todo: dynamically extract the host from the helm chart
	cmp.Host = pulumix.Apply(pulumi.Sprintf("postgres-postgresql.%s.svc.cluster.local", args.Namespace), func(host string) string {
		return host
	})
	cmp.Port = pulumix.Val(5432)

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{
		"username": username,
		"password": password,
	}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}
