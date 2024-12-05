package main

import (
	"errors"
	"fmt"
	"github.com/formancehq/ledger/deployments/pulumi/pkg"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
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

	debug, _ := conf.TryBool("debug")
	imagePullPolicy, _ := conf.Try("image.pullPolicy")

	replicaCount, _ := conf.TryInt("replicaCount")
	experimentalFeatures, _ := conf.TryBool("experimentalFeatures")

	_, err = pulumi_ledger.NewComponent(ctx, "ledger", &pulumi_ledger.ComponentArgs{
		Namespace:       pulumi.String(namespace),
		Timeout:         pulumi.Int(timeout),
		Tag:             pulumi.String(version),
		ImagePullPolicy: pulumi.String(imagePullPolicy),
		Postgres: pulumi_ledger.PostgresArgs{
			URI: pulumi.String(postgresURI),
		},
		Debug:                pulumi.Bool(debug),
		ReplicaCount:         pulumi.Int(replicaCount),
		ExperimentalFeatures: pulumi.Bool(experimentalFeatures),
	})

	return err
}
