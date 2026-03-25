// Operator deployment stack: builds operator images, applies CRDs, deploys operator and optional UI.
package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	k8syaml "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/yaml"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
)

func main() {
	pulumi.Run(func(ctx *pulumi.Context) error {
		cfg := config.New(ctx, "")

		k8s, err := newK8sSetup(ctx, cfg)
		if err != nil {
			return err
		}
		namespace := k8s.Namespace
		k8sProvider := k8s.Provider

		dc := newDockerConfig(ctx, cfg)

		// Build operator image
		ledgerOperatorImage, err := dc.buildImage(ctx, "formancehq/ledger-operator", "../..", "../../Dockerfile")
		if err != nil {
			return fmt.Errorf("failed to build ledger operator image: %w", err)
		}

		// Build operator UI image (optional)
		var operatorUIImage *multiArchImage
		if getConfigBool(cfg, "operatorUI-enabled", true) {
			operatorUIImage, err = dc.buildImage(ctx, "formancehq/ledger-operator-ui", "../../ui", "../../ui/Dockerfile")
			if err != nil {
				return fmt.Errorf("failed to build operator UI image: %w", err)
			}
		}

		// Apply CRDs
		crdFiles, err := filepath.Glob(filepath.Join("..", "..", "chart", "crds", "*.yaml"))
		if err != nil {
			return fmt.Errorf("failed to glob CRD files: %w", err)
		}

		var ledgerCRDs []pulumi.Resource
		for _, crdFile := range crdFiles {
			name := strings.TrimSuffix(filepath.Base(crdFile), filepath.Ext(crdFile))
			crd, crdErr := k8syaml.NewConfigFile(ctx, name+"-crd", &k8syaml.ConfigFileArgs{
				File: crdFile,
			}, pulumi.Provider(k8sProvider))
			if crdErr != nil {
				return fmt.Errorf("failed to apply CRD %s: %w", name, crdErr)
			}
			ledgerCRDs = append(ledgerCRDs, crd)
		}

		// Deploy ledger operator
		operatorChartPath := filepath.Join("..", "..", "chart")
		ledgerOperator, err := helm.NewRelease(ctx, "ledger-operator", &helm.ReleaseArgs{
			Name:      pulumi.String("ledger-operator"),
			Chart:     pulumi.String(operatorChartPath),
			Namespace: namespace.Metadata.Name(),
			Values: pulumi.Map{
				"image": pulumi.Map{
					"repository": pulumi.Sprintf("%s/formancehq/ledger-operator", dc.PullRegistry),
					"tag":        pulumi.Sprintf("latest@%s", ledgerOperatorImage.Digest),
				},
				"leaderElection": pulumi.Bool(true),
				"watchNamespace": namespace.Metadata.Name(),
			},
			ForceUpdate: pulumi.Bool(true),
		},
			pulumi.DependsOn(append([]pulumi.Resource{namespace, ledgerOperatorImage.Resource()}, ledgerCRDs...)),
			pulumi.Provider(k8sProvider),
		)
		if err != nil {
			return fmt.Errorf("failed to deploy ledger operator: %w", err)
		}

		// Deploy operator UI (optional)
		if getConfigBool(cfg, "operatorUI-enabled", true) && operatorUIImage != nil {
			uiChartPath := filepath.Join("..", "..", "ui", "chart")

			uiValues := pulumi.Map{
				"image": pulumi.Map{
					"repository": pulumi.Sprintf("%s/formancehq/ledger-operator-ui", dc.PullRegistry),
					"tag":        pulumi.Sprintf("latest@%s", operatorUIImage.Digest),
				},
			}

			if allowedNS := cfg.Get("operatorUI-allowed-namespaces"); allowedNS != "" {
				uiValues["allowedNamespaces"] = pulumi.String(allowedNS)
			}

			// Configure ingress only when host is provided.
			if ingressHost := cfg.Get("operatorUI-ingress-host"); ingressHost != "" {
				ingressValues := pulumi.Map{
					"enabled": pulumi.Bool(true),
					"hosts": pulumi.MapArray{
						pulumi.Map{
							"host": pulumi.String(ingressHost),
						},
					},
				}
				if ingressClass := cfg.Get("operatorUI-ingress-class"); ingressClass != "" {
					ingressValues["className"] = pulumi.String(ingressClass)
				}
				var ingressAnnotations map[string]any
				if err := cfg.GetObject("operatorUI-ingress-annotations", &ingressAnnotations); err == nil && ingressAnnotations != nil {
					ingressValues["annotations"] = pulumi.ToMap(ingressAnnotations)
				}
				var ingressLabels map[string]any
				if err := cfg.GetObject("operatorUI-ingress-labels", &ingressLabels); err == nil && ingressLabels != nil {
					ingressValues["labels"] = pulumi.ToMap(ingressLabels)
				}

				// DNSEndpoint as an ingress option.
				var dnsEndpointConfig map[string]any
				if err := cfg.GetObject("operatorUI-ingress-dnsEndpoint", &dnsEndpointConfig); err == nil && dnsEndpointConfig != nil {
					dnsEndpointConfig["enabled"] = true
					ingressValues["dnsEndpoint"] = pulumi.ToMap(dnsEndpointConfig)
				}

				uiValues["ingress"] = ingressValues
			}

			if getConfigBool(cfg, "operatorUI-auth-enabled", false) {
				authValues := pulumi.Map{
					"enabled":   pulumi.Bool(true),
					"issuerUrl": pulumi.String(cfg.Require("operatorUI-auth-issuer-url")),
					"clientId":  pulumi.String(cfg.Require("operatorUI-auth-client-id")),
				}
				authValues["clientSecret"] = config.GetSecret(ctx, "operatorUI-auth-client-secret")
				authValues["sessionSecret"] = config.GetSecret(ctx, "operatorUI-auth-session-secret")

				if redirectUri := cfg.Get("operatorUI-auth-redirect-uri"); redirectUri != "" {
					authValues["redirectUri"] = pulumi.String(redirectUri)
				}
				if scopes := cfg.Get("operatorUI-auth-scopes"); scopes != "" {
					authValues["scopes"] = pulumi.String(scopes)
				}

				var roleMapping map[string]any
				if err := cfg.GetObject("operatorUI-auth-role-mapping", &roleMapping); err == nil && roleMapping != nil {
					authValues["roleMapping"] = pulumi.ToMap(roleMapping)
				}

				uiValues["auth"] = authValues
			}

			operatorUI, uiErr := helm.NewRelease(ctx, "ledger-operator-ui", &helm.ReleaseArgs{
				Name:        pulumi.String("ledger-operator-ui"),
				Chart:       pulumi.String(uiChartPath),
				Namespace:   namespace.Metadata.Name(),
				Values:      uiValues,
				ForceUpdate: pulumi.Bool(true),
			},
				pulumi.DependsOn(append([]pulumi.Resource{namespace, ledgerOperator, operatorUIImage.Resource()}, ledgerCRDs...)),
				pulumi.Provider(k8sProvider),
			)
			if uiErr != nil {
				return fmt.Errorf("failed to deploy operator UI: %w", uiErr)
			}
			ctx.Export("operatorUIRelease", operatorUI.Name)
			ctx.Export("operatorUIImage", pulumi.Sprintf("%s/formancehq/ledger-operator-ui:latest@%s", dc.PullRegistry, operatorUIImage.Digest))
		}

		// Exports
		ctx.Export("namespace", namespace.Metadata.Name())
		ctx.Export("ledgerOperatorImage", pulumi.Sprintf("%s/formancehq/ledger-operator:latest@%s", dc.PullRegistry, ledgerOperatorImage.Digest))
		ctx.Export("ledgerOperatorRelease", ledgerOperator.Name)

		return nil
	})
}
