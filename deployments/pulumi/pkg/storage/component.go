package storage

import (
	"context"
	"errors"
	"fmt"
	. "github.com/formancehq/ledger/deployments/pulumi/pkg/common"
	. "github.com/formancehq/ledger/deployments/pulumi/pkg/utils"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
	"net/url"
	"slices"
	"strings"
	"time"
)

type ConnectivityDatabaseArgs struct {
	AWSEnableIAM    pulumix.Input[bool]
	MaxIdleConns    pulumix.Input[*int]
	MaxOpenConns    pulumix.Input[*int]
	ConnMaxIdleTime pulumix.Input[*time.Duration]
	Options         pulumix.Output[map[string]string]
}

type databaseComponent interface {
	pulumi.ComponentResource

	GetEndpoint() pulumix.Input[string]
	GetUsername() pulumix.Input[string]
	GetPassword() pulumix.Input[string]
	GetPort() pulumix.Input[int]
	GetOptions() pulumix.Input[map[string]string]
	GetDatabase() pulumix.Input[string]
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

type Service struct {
	Annotations pulumix.Input[map[string]string]
}

type Args struct {
	Postgres                 *PostgresDatabaseArgs
	RDS                      *RDSDatabaseArgs
	ConnectivityDatabaseArgs ConnectivityDatabaseArgs
	DisableUpgrade           pulumix.Input[bool]
	Service                  Service
}

func (args *Args) SetDefaults() {
	if args.RDS == nil && args.Postgres == nil {
		args.Postgres = &PostgresDatabaseArgs{
			Install: &PostgresInstallArgs{},
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

func (cmp *Component) GetEnvVars() corev1.EnvVarArray {
	ret := corev1.EnvVarArray{
		corev1.EnvVarArgs{
			Name: pulumi.String("POSTGRES_USERNAME"),
			ValueFrom: &corev1.EnvVarSourceArgs{
				SecretKeyRef: &corev1.SecretKeySelectorArgs{
					Name: cmp.Credentials.Metadata.Name(),
					Key:  pulumi.String("username"),
				},
			},
		},
		corev1.EnvVarArgs{
			Name: pulumi.String("POSTGRES_PASSWORD"),
			ValueFrom: &corev1.EnvVarSourceArgs{
				SecretKeyRef: &corev1.SecretKeySelectorArgs{
					Name: cmp.Credentials.Metadata.Name(),
					Key:  pulumi.String("password"),
				},
			},
		},
		corev1.EnvVarArgs{
			Name: pulumi.String("POSTGRES_URI"),
			Value: pulumi.Sprintf("postgres://$(POSTGRES_USERNAME):$(POSTGRES_PASSWORD)@%s.%s.svc.cluster.local:%d/%s?%s",
				cmp.Service.Metadata.Name().Elem(),
				cmp.Service.Metadata.Namespace().Elem(),
				cmp.DatabaseComponent.GetPort(),
				cmp.DatabaseComponent.GetDatabase(),
				pulumix.Apply2(
					cmp.DatabaseComponent.GetOptions(),
					cmp.Options,
					func(options, additionalOptions map[string]string) string {
						if options == nil {
							return ""
						}

						asSlice := make([]string, 0, len(options))
						for k, v := range options {
							asSlice = append(asSlice, fmt.Sprintf("%s=%s", k, v))
						}
						for k, v := range additionalOptions {
							asSlice = append(asSlice, fmt.Sprintf("%s=%s", k, v))
						}
						slices.Sort(asSlice)

						return strings.Join(asSlice, "&")
					},
				),
			),
		},
	}

	if cmp.AWSEnableIAM != nil {
		ret = append(ret, corev1.EnvVarArgs{
			Name:  pulumi.String("POSTGRES_AWS_ENABLE_IAM"),
			Value: BoolToString(cmp.AWSEnableIAM).Untyped().(pulumi.StringOutput),
		})
	}

	if cmp.ConnMaxIdleTime != nil {
		ret = append(ret, corev1.EnvVarArgs{
			Name: pulumi.String("POSTGRES_CONN_MAX_IDLE_TIME"),
			Value: pulumix.Apply(cmp.ConnMaxIdleTime, func(connMaxIdleTime *time.Duration) string {
				if connMaxIdleTime == nil {
					return ""
				}
				return connMaxIdleTime.String()
			}).Untyped().(pulumi.StringOutput),
		})
	}

	if cmp.MaxOpenConns != nil {
		ret = append(ret, corev1.EnvVarArgs{
			Name: pulumi.String("POSTGRES_MAX_OPEN_CONNS"),
			Value: pulumix.Apply(cmp.MaxOpenConns, func(maxOpenConns *int) string {
				if maxOpenConns == nil {
					return ""
				}
				return fmt.Sprint(*maxOpenConns)
			}).Untyped().(pulumi.StringOutput),
		})
	}

	if cmp.MaxIdleConns != nil {
		ret = append(ret, corev1.EnvVarArgs{
			Name: pulumi.String("POSTGRES_MAX_IDLE_CONNS"),
			Value: pulumix.Apply(cmp.MaxIdleConns, func(maxIdleConns *int) string {
				if maxIdleConns == nil {
					return ""
				}
				return fmt.Sprint(*maxIdleConns)
			}).Untyped().(pulumi.StringOutput),
		})
	}

	return ret
}

func (cmp *Component) GetDevBoxContainer(ctx context.Context) corev1.ContainerInput {
	return corev1.ContainerArgs{
		Name:  pulumi.String("psql"),
		Image: pulumi.String("alpine/psql:17.4"),
		Command: pulumi.StringArray{
			pulumi.String("sleep"),
		},
		Args: pulumi.StringArray{
			pulumi.String("infinity"),
		},
		Env: corev1.EnvVarArray{
			corev1.EnvVarArgs{
				Name:  pulumi.String("POSTGRES_SERVICE_NAME"),
				Value: cmp.Service.Metadata.Name(),
			},
			corev1.EnvVarArgs{
				Name: pulumi.String("POSTGRES_USERNAME"),
				ValueFrom: corev1.EnvVarSourceArgs{
					SecretKeyRef: &corev1.SecretKeySelectorArgs{
						Key:  pulumi.String("username"),
						Name: cmp.Credentials.Metadata.Name(),
					},
				},
			},
			corev1.EnvVarArgs{
				Name: pulumi.String("POSTGRES_PASSWORD"),
				ValueFrom: corev1.EnvVarSourceArgs{
					SecretKeyRef: &corev1.SecretKeySelectorArgs{
						Key:  pulumi.String("password"),
						Name: cmp.Credentials.Metadata.Name(),
					},
				},
			},
			corev1.EnvVarArgs{
				Name: pulumi.String("PGDATABASE"),
				Value: cmp.DatabaseComponent.GetDatabase().
					ToOutput(ctx).
					Untyped().(pulumi.StringOutput),
			},
			corev1.EnvVarArgs{
				Name:  pulumi.String("PGPASSWORD"),
				Value: pulumi.String("$(POSTGRES_PASSWORD)"),
			},
			corev1.EnvVarArgs{
				Name:  pulumi.String("PGHOST"),
				Value: pulumi.String("$(POSTGRES_SERVICE_NAME)"),
			},
			corev1.EnvVarArgs{
				Name:  pulumi.String("PGUSER"),
				Value: pulumi.String("$(POSTGRES_USERNAME)"),
			},
		},
	}
}

func NewComponent(ctx *pulumi.Context, name string, args ComponentArgs, options ...pulumi.ResourceOption) (*Component, error) {
	cmp := &Component{
		ConnectivityDatabaseArgs: args.ConnectivityDatabaseArgs,
	}
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
			"password": pulumix.Apply(cmp.DatabaseComponent.GetPassword(), func(password string) string {
				return url.QueryEscape(password)
			}).Untyped().(pulumi.StringOutput),
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
				Annotations: args.Service.Annotations.
					ToOutput(ctx.Context()).
					Untyped().(pulumi.StringMapOutput),
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

	args.DisableUpgrade.ToOutput(ctx.Context()).ApplyT(func(disableUpgrade bool) error {
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
