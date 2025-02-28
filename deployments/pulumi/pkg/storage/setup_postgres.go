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
	Install pulumix.Input[bool]
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
		})
	}

	install, err := internals.UnsafeAwaitOutput(ctx.Context(), a.Install.ToOutput(ctx.Context()))
	if err != nil {
		return nil, fmt.Errorf("awaiting install: %w", err)
	}
	if install.Value != nil && !install.Value.(bool) {
		panic("uri must be provided if install is false")
	}

	return newPostgresComponent(ctx, "postgres", &PostgresComponentArgs{
		Namespace: args.Namespace,
	}, options...)
}

func (a *PostgresDatabaseArgs) SetDefaults() {}
