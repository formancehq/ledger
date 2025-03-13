package storage

import (
	"errors"
	"fmt"
	. "github.com/formancehq/ledger/deployments/pulumi/pkg/utils"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
	"slices"
	"strings"
	"time"
)

type ConnectivityDatabaseArgs struct {
	AWSEnableIAM    pulumix.Input[bool]
	MaxIdleConns    pulumix.Input[*int]
	MaxOpenConns    pulumix.Input[*int]
	ConnMaxIdleTime pulumix.Input[*time.Duration]
}

type databaseComponent interface {
	pulumi.ComponentResource

	GetEndpoint() pulumix.Input[string]
	GetUsername() pulumix.Input[string]
	GetPassword() pulumix.Input[string]
	GetPort() pulumix.Input[int]
	GetOptions() pulumix.Input[map[string]string]
}

type databaseWithOwnedServiceComponent interface {
	GetService() *corev1.Service
}

type factoryArgs struct {
	CommonArgs
	Migrated pulumix.Input[string]
}

type databaseComponentFactory interface {
	setup(ctx *pulumi.Context, args factoryArgs, options ...pulumi.ResourceOption) (databaseComponent, error)
}

type Args struct {
	Postgres                 *PostgresDatabaseArgs
	RDS                      *RDSDatabaseArgs
	ConnectivityDatabaseArgs ConnectivityDatabaseArgs
	DisableUpgrade           pulumix.Output[bool]
}

func (args *Args) SetDefaults() {
	if args.RDS == nil && args.Postgres == nil {
		args.Postgres = &PostgresDatabaseArgs{
			Install: pulumix.Val(true),
		}
	}
	if args.RDS != nil {
		args.RDS.SetDefaults()
	}
	if args.Postgres != nil {
		args.Postgres.SetDefaults()
	}
}

type ComponentArgs struct {
	CommonArgs CommonArgs
	Args
}

func (args *ComponentArgs) SetDefaults() {
	args.Args.SetDefaults()
}

type Component struct {
	pulumi.ResourceState

	ConnectivityDatabaseArgs
	DatabaseComponent databaseComponent
	Credentials       *corev1.Secret
	Service           *corev1.Service
}

func (args *Component) GetEnvVars() corev1.EnvVarArray {
	ret := corev1.EnvVarArray{
		corev1.EnvVarArgs{
			Name: pulumi.String("POSTGRES_USERNAME"),
			ValueFrom: &corev1.EnvVarSourceArgs{
				SecretKeyRef: &corev1.SecretKeySelectorArgs{
					Name: args.Credentials.Metadata.Name(),
					Key:  pulumi.String("username"),
				},
			},
		},
		corev1.EnvVarArgs{
			Name: pulumi.String("POSTGRES_PASSWORD"),
			ValueFrom: &corev1.EnvVarSourceArgs{
				SecretKeyRef: &corev1.SecretKeySelectorArgs{
					Name: args.Credentials.Metadata.Name(),
					Key:  pulumi.String("password"),
				},
			},
		},
		corev1.EnvVarArgs{
			Name: pulumi.String("POSTGRES_URI"),
			Value: pulumi.Sprintf("postgres://$(POSTGRES_USERNAME):$(POSTGRES_PASSWORD)@%s:%d/postgres?%s",
				args.DatabaseComponent.GetEndpoint(),
				args.DatabaseComponent.GetPort(),
				pulumix.Apply(args.DatabaseComponent.GetOptions(), func(options map[string]string) string {
					if options == nil {
						return ""
					}

					asSlice := make([]string, 0, len(options))
					for k, v := range options {
						asSlice = append(asSlice, fmt.Sprintf("%s=%s", k, v))
					}
					slices.Sort(asSlice)

					return strings.Join(asSlice, "&")
				}),
			),
		},
	}

	if args.AWSEnableIAM != nil {
		ret = append(ret, corev1.EnvVarArgs{
			Name:  pulumi.String("POSTGRES_AWS_ENABLE_IAM"),
			Value: BoolToString(args.AWSEnableIAM).Untyped().(pulumi.StringOutput),
		})
	}

	if args.ConnMaxIdleTime != nil {
		ret = append(ret, corev1.EnvVarArgs{
			Name: pulumi.String("POSTGRES_CONN_MAX_IDLE_TIME"),
			Value: pulumix.Apply(args.ConnMaxIdleTime, func(connMaxIdleTime *time.Duration) string {
				if connMaxIdleTime == nil {
					return ""
				}
				return connMaxIdleTime.String()
			}).Untyped().(pulumi.StringOutput),
		})
	}

	if args.MaxOpenConns != nil {
		ret = append(ret, corev1.EnvVarArgs{
			Name: pulumi.String("POSTGRES_MAX_OPEN_CONNS"),
			Value: pulumix.Apply(args.MaxOpenConns, func(maxOpenConns *int) string {
				if maxOpenConns == nil {
					return ""
				}
				return fmt.Sprint(*maxOpenConns)
			}).Untyped().(pulumi.StringOutput),
		})
	}

	if args.MaxIdleConns != nil {
		ret = append(ret, corev1.EnvVarArgs{
			Name: pulumi.String("POSTGRES_MAX_IDLE_CONNS"),
			Value: pulumix.Apply(args.MaxIdleConns, func(maxIdleConns *int) string {
				if maxIdleConns == nil {
					return ""
				}
				return fmt.Sprint(*maxIdleConns)
			}).Untyped().(pulumi.StringOutput),
		})
	}

	return ret
}

func NewComponent(ctx *pulumi.Context, name string, args ComponentArgs, options ...pulumi.ResourceOption) (*Component, error) {
	cmp := &Component{}
	err := ctx.RegisterComponentResource("Formance:Ledger:Storage", name, cmp, options...)
	if err != nil {
		return nil, err
	}

	var factory databaseComponentFactory
	switch {
	case args.Postgres != nil && args.RDS != nil:
		return nil, errors.New("either Postgres or RDS config must be provided, not both")
	case args.Postgres != nil:
		factory = args.Postgres
	case args.RDS != nil:
		factory = args.RDS
	default:
		return nil, errors.New("either Postgres or RDS config must be provided")
	}

	migrated, resolveMigrated := pulumi.DeferredOutput[string](ctx.Context())

	cmp.DatabaseComponent, err = factory.setup(ctx, factoryArgs{
		CommonArgs: args.CommonArgs,
		Migrated:   migrated,
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, err
	}

	cmp.Credentials, err = corev1.NewSecret(ctx, "database-credentials", &corev1.SecretArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Name: pulumi.Sprintf("%s-credentials", name).ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
			Namespace: args.CommonArgs.Namespace.
				ToOutput(ctx.Context()).
				Untyped().(pulumi.StringOutput),
		},
		StringData: pulumi.StringMap{
			"username": cmp.DatabaseComponent.GetUsername().ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
			"password": cmp.DatabaseComponent.GetPassword().ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
		},
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, fmt.Errorf("creating secret: %w", err)
	}

	if v, ok := cmp.DatabaseComponent.(databaseWithOwnedServiceComponent); ok {
		cmp.Service = v.GetService()
	} else {
		cmp.Service, err = corev1.NewService(ctx, "database-service", &corev1.ServiceArgs{
			Metadata: &metav1.ObjectMetaArgs{
				Name: pulumi.String("storage"),
				Namespace: args.CommonArgs.Namespace.
					ToOutput(ctx.Context()).
					Untyped().(pulumi.StringOutput),
			},
			Spec: &corev1.ServiceSpecArgs{
				Type: pulumi.String("ExternalName"),
				ExternalName: cmp.DatabaseComponent.
					GetEndpoint().
					ToOutput(ctx.Context()).
					Untyped().(pulumi.StringOutput),
			},
		}, pulumi.Parent(cmp))
		if err != nil {
			return nil, fmt.Errorf("creating service: %w", err)
		}
	}

	args.DisableUpgrade.ApplyT(func(disableUpgrade bool) error {
		if disableUpgrade {
			return nil
		}
		job, err := runMigrateJob(ctx, migrationArgs{
			CommonArgs: args.CommonArgs,
			component:  cmp,
		},
			pulumi.Parent(cmp),
			pulumi.DependsOn([]pulumi.Resource{
				cmp.DatabaseComponent.(pulumi.Resource),
				cmp.Credentials,
				cmp.Service,
			}),
		)
		if err != nil {
			return fmt.Errorf("creating migration job: %w", err)
		}
		job.Status.Succeeded().Elem().ApplyT(func(succeeded int) error {
			if succeeded == 0 {
				return errors.New("migration job failed")
			}
			resolveMigrated(args.CommonArgs.Tag.ToOutput(ctx.Context()))
			return nil
		})

		return nil
	})

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}
