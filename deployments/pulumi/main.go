package main

import (
	"errors"
	"fmt"
	helm "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
)

func main() {
	pulumi.Run(deployLedger)
}

func deployLedger(ctx *pulumi.Context) error {

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

	rel, err := helm.NewRelease(ctx, "ledger", &helm.ReleaseArgs{
		Chart:           pulumi.String("../helm"),
		Namespace:       pulumi.String(namespace),
		CreateNamespace: pulumi.BoolPtr(true),
		Timeout:         pulumi.IntPtr(timeout),
		Values: pulumi.Map(map[string]pulumi.Input{
			"image": pulumi.Map{
				"repository": pulumi.String("ghcr.io/formancehq/ledger"),
				"tag":        pulumi.String(version),
				"pullPolicy": pulumi.String(imagePullPolicy),
			},
			"postgres": pulumi.Map{
				"uri": pulumi.String(postgresURI),
			},
			"debug":        pulumi.Bool(debug),
			"replicaCount": pulumi.Int(replicaCount),
			"experimentalFeatures": pulumi.Bool(experimentalFeatures),
		}),
	})
	if err != nil {
		return err
	}

	ctx.Export("service-name", rel.Status.Name())
	ctx.Export("service-namespace", rel.Status.Namespace())
	ctx.Export("service-port", pulumi.Int(8080))

	return err
}
