// Testing stack: k6-operator, benchmark-operator.
package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/formancehq/ledger-v3-poc/deployments/devenv/shared"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	k8syaml "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/yaml"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
)

func main() {
	pulumi.Run(func(ctx *pulumi.Context) error {
		cfg := config.New(ctx, "")

		k8s, err := shared.NewK8sSetup(ctx, cfg)
		if err != nil {
			return err
		}
		namespace := k8s.Namespace
		k8sProvider := k8s.Provider

		dc := shared.NewDockerConfig(ctx, cfg)

		// Deploy k6-operator (optional, enabled by default)
		var k6Operator *helm.Release
		if shared.GetConfigBool(cfg, "k6operator-enabled", true) {
			k6OperatorValues, k6Err := shared.GetConfigObject(cfg, "k6operator", ".")
			if k6Err != nil {
				k6OperatorValues = make(map[string]any)
			}
			k6Operator, err = helm.NewRelease(ctx, "k6-operator", &helm.ReleaseArgs{
				Name:           pulumi.String("k6-operator"),
				Chart:          pulumi.String("k6-operator"),
				RepositoryOpts: &helm.RepositoryOptsArgs{Repo: pulumi.String("https://grafana.github.io/helm-charts")},
				Namespace:      namespace.Metadata.Name(),
				Values:         pulumi.ToMap(k6OperatorValues),
				ForceUpdate:    pulumi.Bool(true),
			},
				pulumi.DependsOn([]pulumi.Resource{namespace}),
				pulumi.Provider(k8sProvider),
			)
			if err != nil {
				return fmt.Errorf("failed to deploy k6-operator: %w", err)
			}
			ctx.Export("k6OperatorRelease", k6Operator.Name)
		}

		// Deploy benchmark operator (optional, disabled by default)
		if shared.GetConfigBool(cfg, "benchmarkOperator-enabled", false) {
			benchmarkOperatorImage, imgErr := dc.BuildImage(ctx, "formancehq/benchmark-operator", "../../benchmark-operator", "../../benchmark-operator/Dockerfile")
			if imgErr != nil {
				return fmt.Errorf("failed to build benchmark operator image: %w", imgErr)
			}

			benchmarkOperatorValues, valErr := shared.GetConfigObject(cfg, "benchmarkOperator", ".")
			if valErr != nil {
				benchmarkOperatorValues = make(map[string]any)
			}
			var imageConfiguration map[string]any
			if benchmarkOperatorValues["image"] == nil {
				imageConfiguration = map[string]any{}
				benchmarkOperatorValues["image"] = imageConfiguration
			} else {
				imageConfiguration = benchmarkOperatorValues["image"].(map[string]any)
			}

			imageConfiguration["repository"] = pulumi.Sprintf("%s/formancehq/benchmark-operator", dc.PullRegistry)
			imageConfiguration["tag"] = pulumi.Sprintf("latest@%s", benchmarkOperatorImage.Digest)

			// Inject RBAC rules for ledger resources so the benchmark operator
			// can create/delete LedgerService and Ledger CRs.
			var rbacConfiguration map[string]any
			if benchmarkOperatorValues["rbac"] == nil {
				rbacConfiguration = map[string]any{}
				benchmarkOperatorValues["rbac"] = rbacConfiguration
			} else {
				rbacConfiguration = benchmarkOperatorValues["rbac"].(map[string]any)
			}
			rbacConfiguration["additionalRules"] = []map[string]any{
				{
					"apiGroups": []string{"ledger.formance.com"},
					"resources": []string{"ledgerservices", "ledgers"},
					"verbs":     []string{"get", "list", "watch", "create", "delete"},
				},
			}

			benchmarkChartPath := filepath.Join("..", "..", "benchmark-operator", "chart")

			// Apply CRDs explicitly so they are updated on every deploy
			// (Helm only installs CRDs on first install, never updates them).
			crdFiles, crdErr := filepath.Glob(filepath.Join(benchmarkChartPath, "crds", "*.yaml"))
			if crdErr != nil {
				return fmt.Errorf("failed to glob benchmark CRD files: %w", crdErr)
			}
			var benchmarkCRDs []pulumi.Resource
			for _, crdFile := range crdFiles {
				name := strings.TrimSuffix(filepath.Base(crdFile), filepath.Ext(crdFile))
				crd, crdApplyErr := k8syaml.NewConfigFile(ctx, name+"-crd", &k8syaml.ConfigFileArgs{
					File: crdFile,
				}, pulumi.Provider(k8sProvider))
				if crdApplyErr != nil {
					return fmt.Errorf("failed to apply CRD %s: %w", name, crdApplyErr)
				}
				benchmarkCRDs = append(benchmarkCRDs, crd)
			}

			benchmarkDeps := append([]pulumi.Resource{namespace, benchmarkOperatorImage.Resource()}, benchmarkCRDs...)
			if k6Operator != nil {
				benchmarkDeps = append(benchmarkDeps, k6Operator)
			}

			benchmarkOperator, benchErr := helm.NewRelease(ctx, "benchmark-operator", &helm.ReleaseArgs{
				Name:             pulumi.String("benchmark-operator"),
				Chart:            pulumi.String(benchmarkChartPath),
				Namespace:        namespace.Metadata.Name(),
				Values:           pulumi.ToMap(benchmarkOperatorValues),
				DependencyUpdate: pulumi.Bool(true),
				ForceUpdate:      pulumi.Bool(true),
			},
				pulumi.DependsOn(benchmarkDeps),
				pulumi.Provider(k8sProvider),
			)
			if benchErr != nil {
				return fmt.Errorf("failed to deploy benchmark operator: %w", benchErr)
			}
			ctx.Export("benchmarkOperatorRelease", benchmarkOperator.Name)
			ctx.Export("benchmarkOperatorImage", pulumi.Sprintf("%s/formancehq/benchmark-operator:latest@%s", dc.PullRegistry, benchmarkOperatorImage.Digest))
		}

		// Exports
		ctx.Export("namespace", namespace.Metadata.Name())

		return nil
	})
}
