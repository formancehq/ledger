package main

import (
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/deployments/pulumi/pkg"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

func main() {
	pulumi.Run(deploy)
}

func deploy(ctx *pulumi.Context) error {
	conf := config.New(ctx, "")
	postgresURI := conf.Require("postgres.uri")

	namespace, err := conf.Try("namespace")
	if err != nil {
		namespace = "default"
	}

	version, err := conf.Try("version")
	if err != nil {
		version = "latest"
	}

	timeout, err := conf.TryInt("timeout")
	if err != nil {
		if errors.Is(err, config.ErrMissingVar) {
			timeout = 60
		} else {
			return fmt.Errorf("error reading timeout: %w", err)
		}
	}

	_, err = pulumi_ledger.NewComponent(ctx, "ledger", &pulumi_ledger.ComponentArgs{
		Namespace:       pulumi.String(namespace),
		Timeout:         pulumi.Int(timeout),
		Tag:             pulumi.String(version),
		ImagePullPolicy: pulumi.String(conf.Get("image.pullPolicy")),
		Postgres: pulumi_ledger.PostgresArgs{
			URI: pulumi.String(postgresURI),
		},
		Debug: pulumi.Bool(conf.GetBool("debug")),
		API: pulumi_ledger.APIArgs{
			ReplicaCount:         pulumix.Val(pointer.For(conf.GetInt("replicaCount"))),
			ExperimentalFeatures: pulumi.Bool(conf.GetBool("experimentalFeatures")),
		},
	})

	return err
}
