package main

import (
	"fmt"
	pulumi_postgres "github.com/formancehq/ledger/deployments/pulumi/postgres/pkg"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
)

func main() {
	pulumi.Run(func(ctx *pulumi.Context) error {

		conf := config.New(ctx, "")
		namespace, err := conf.Try("namespace")
		if err != nil {
			namespace = "default"
		}
		_, err = pulumi_postgres.NewPostgresComponent(ctx, "postgres", &pulumi_postgres.PostgresComponentArgs{
			Namespace: pulumi.String(namespace),
		})
		if err != nil {
			return fmt.Errorf("creating postgres component: %w", err)
		}

		return nil
	})
}
