package storage

import (
	"errors"
	"fmt"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	helm "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v4"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/internals"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type PostgresDatabaseComponent struct {
	pulumi.ResourceState

	Username pulumix.Output[string]
	Password pulumix.Output[string]

	Chart     *helm.Chart
	Service   *corev1.Service
	Namespace pulumix.Input[string]
}

func (cmp *PostgresDatabaseComponent) GetDatabase() pulumix.Input[string] {
	return pulumix.Val("postgres")
}

func (cmp *PostgresDatabaseComponent) GetService() *corev1.Service {
	return cmp.Service
}

func (cmp *PostgresDatabaseComponent) GetOptions() pulumix.Input[map[string]string] {
	return pulumix.Val(map[string]string{})
}

func (cmp *PostgresDatabaseComponent) GetEndpoint() pulumix.Input[string] {
	return pulumix.Apply(
		cmp.Namespace,
		func(namespace string) string {
			return fmt.Sprintf("postgres-postgresql.%s.svc.cluster.local", namespace)
		})
}

func (cmp *PostgresDatabaseComponent) GetUsername() pulumix.Input[string] {
	return cmp.Username
}

func (cmp *PostgresDatabaseComponent) GetPassword() pulumix.Input[string] {
	return cmp.Password
}

func (cmp *PostgresDatabaseComponent) GetPort() pulumix.Input[int] {
	return pulumix.Val(5432)
}

type PostgresInstallArgs struct {
	Username pulumix.Input[string]
	Password pulumix.Input[string]
}

func (args *PostgresInstallArgs) SetDefaults() {
	if args.Username == nil {
		args.Username = pulumix.Val("")
	}
	args.Username = pulumix.Apply(args.Username, func(username string) string {
		if username == "" {
			return "root"
		}
		return username
	})
	if args.Password == nil {
		args.Password = pulumix.Val("")
	}
	args.Password = pulumix.Apply(args.Password, func(password string) string {
		if password == "" {
			return "password"
		}
		return password
	})
}

type PostgresComponentArgs struct {
	Namespace pulumix.Input[string]
	PostgresInstallArgs
}

func newPostgresComponent(ctx *pulumi.Context, name string, args *PostgresComponentArgs, opts ...pulumi.ResourceOption) (*PostgresDatabaseComponent, error) {
	cmp := &PostgresDatabaseComponent{}
	err := ctx.RegisterComponentResource("Formance:Ledger:Postgres", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	cmp.Namespace = args.Namespace

	cmp.Chart, err = helm.NewChart(ctx, "postgres", &helm.ChartArgs{
		Chart:     pulumi.String("oci://registry-1.docker.io/bitnamicharts/postgresql"),
		Version:   pulumi.String("16.4.7"),
		Name:      pulumi.String("postgres"),
		Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
		Values: pulumi.Map{
			"global": pulumi.Map{
				"postgresql": pulumi.Map{
					"auth": pulumi.Map{
						"username": args.Username,
						"password": args.Password,
					},
				},
			},
		},
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, err
	}

	ret, err := internals.UnsafeAwaitOutput(ctx.Context(), pulumix.ApplyErr(cmp.Chart.Resources, func(resources []any) (*corev1.Service, error) {
		for _, resource := range resources {
			service, ok := resource.(*corev1.Service)
			if !ok {
				continue
			}
			ret, err := internals.UnsafeAwaitOutput(ctx.Context(), pulumix.Apply2(
				service.Spec.Type().Elem(),
				service.Spec.ClusterIP().Elem(),
				func(serviceType, clusterIP string) *corev1.Service {
					// select not headless service
					if serviceType != "ClusterIP" || clusterIP == "None" {
						return nil
					}
					return service
				},
			))
			if err != nil {
				return nil, err
			}
			if ret.Value != nil {
				return ret.Value.(*corev1.Service), nil
			}
		}
		return nil, errors.New("not found")
	}))
	if err != nil {
		return nil, err
	}

	cmp.Service = ret.Value.(*corev1.Service)
	cmp.Username = args.Username.ToOutput(ctx.Context())
	cmp.Password = args.Password.ToOutput(ctx.Context())

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}

var _ databaseComponent = (*PostgresDatabaseComponent)(nil)
var _ databaseWithOwnedServiceComponent = (*PostgresDatabaseComponent)(nil)
