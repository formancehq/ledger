package storage

import (
	"fmt"
	"github.com/kos-v/dsnparser"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/internals"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
	"strconv"
)

type PostgresDatabaseArgs struct {
	URI     pulumix.Input[string]
	Install *PostgresInstallArgs
}

func (a PostgresDatabaseArgs) setup(ctx *pulumi.Context, args factoryArgs, options ...pulumi.ResourceOption) (databaseComponent, error) {

	uri, err := internals.UnsafeAwaitOutput(ctx.Context(), a.URI.ToOutput(ctx.Context()))
	if err != nil {
		return nil, fmt.Errorf("awaiting URI: %w", err)
	}
	if uri.Value != nil && uri.Value.(string) != "" {
		dsn := dsnparser.Parse(uri.Value.(string))
		port, err := strconv.Atoi(dsn.GetPort())
		if err != nil {
			return nil, fmt.Errorf("parsing port: %w", err)
		}

		return newExternalDatabaseComponent(ctx, "postgres", ExternalDatabaseComponentArgs{
			Endpoint: pulumix.Val(dsn.GetHost()),
			Username: pulumix.Val(dsn.GetUser()),
			Password: pulumix.Val(dsn.GetPassword()),
			Port:     pulumix.Val(port),
			Options:  pulumix.Val(dsn.GetParams()),
			Database: pulumix.Val(dsn.GetPath()),
		})
	}

	return newPostgresComponent(ctx, "postgres", &PostgresComponentArgs{
		Namespace:           args.Namespace,
		PostgresInstallArgs: *a.Install,
	}, options...)
}

func (a *PostgresDatabaseArgs) SetDefaults() {
	if a.URI == nil && a.Install == nil {
		a.Install = &PostgresInstallArgs{}
	}
	if a.URI == nil {
		a.URI = pulumix.Val("")
	}
	if a.Install != nil {
		a.Install.SetDefaults()
	}
}
