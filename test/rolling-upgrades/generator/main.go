package main

import (
	"github.com/formancehq/ledger/test/rolling-upgrades/generator/pkg"
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
		image := conf.Get("image")
		if image == "" {
			image = "ghcr.io/formancehq/ledger-generator:latest"
		}
		ledgerURL := conf.Require("ledger-url")

		_, err = pulumi_generator.NewGeneratorComponent(ctx, "generator", &pulumi_generator.GeneratorComponentArgs{
			Namespace: pulumi.String(namespace),
			LedgerURL: pulumi.String(ledgerURL),
			Image:     pulumi.String(image),
		})
		if err != nil {
			return err
		}

		return nil
	})
}
