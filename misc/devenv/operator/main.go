// Operator stack: Docker images, CRDs, ledger-operator, operator-ui, LedgerDefaults, cold storage.
package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/formancehq/ledger-v3-poc/deployments/devenv/shared"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/iam"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/s3"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apiextensions"
	v1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
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
		buildVersion := shared.GetBuildVersion("../../..")
		_ = ctx.Log.Info(fmt.Sprintf("Build version: %s", buildVersion), nil)

		// Build Docker images
		dockerImage, err := dc.BuildImage(ctx, "formancehq/ledger-exp", "../../..", "../../../Dockerfile")
		if err != nil {
			return fmt.Errorf("failed to build Docker image: %w", err)
		}

		ledgerOperatorImage, err := dc.BuildImage(ctx, "formancehq/ledger-operator", "../../operator", "../../operator/Dockerfile")
		if err != nil {
			return fmt.Errorf("failed to build ledger operator image: %w", err)
		}

		var operatorUIImage *shared.MultiArchImage
		if shared.GetConfigBool(cfg, "operatorUI-enabled", true) {
			operatorUIImage, err = dc.BuildImage(ctx, "formancehq/ledger-operator-ui", "../../operator/ui", "../../operator/ui/Dockerfile")
			if err != nil {
				return fmt.Errorf("failed to build operator UI image: %w", err)
			}
		}

		// Apply CRDs
		crdFiles, err := filepath.Glob(filepath.Join("..", "..", "operator", "chart", "crds", "*.yaml"))
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
		operatorChartPath := filepath.Join("..", "..", "operator", "chart")
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
		if shared.GetConfigBool(cfg, "operatorUI-enabled", true) && operatorUIImage != nil {
			uiChartPath := filepath.Join("..", "..", "operator", "ui", "chart")

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

			if shared.GetConfigBool(cfg, "operatorUI-auth-enabled", false) {
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

		// LedgerDefaults CR
		ledgerSpec, err := shared.GetConfigObject(cfg, "ledger", ".")
		if err != nil {
			return fmt.Errorf("failed to read Ledger spec: %w", err)
		}
		if ledgerSpec == nil {
			ledgerSpec = make(map[string]any)
		}

		ledgerSpec["image"] = pulumi.Map{
			"repository": pulumi.Sprintf("%s/formancehq/ledger-exp", dc.PullRegistry),
			"tag":        pulumi.Sprintf("latest@%s", dockerImage.Digest),
		}

		// Add build version to Pyroscope tags.
		if configSection, ok := ledgerSpec["config"].(map[string]any); ok {
			if monitoring, ok := configSection["monitoring"].(map[string]any); ok {
				if pyroscopeCfg, ok := monitoring["pyroscope"].(map[string]any); ok {
					existingTags := ""
					if tags, ok := pyroscopeCfg["tags"].(string); ok && tags != "" {
						existingTags = tags + ","
					}
					pyroscopeCfg["tags"] = fmt.Sprintf("%sversion=%s", existingTags, buildVersion)
				}
			}
		}

		defaultsSpec := shared.ExtractLedgerDefaults(ledgerSpec)
		shared.EnsurePersistenceRetentionPolicy(defaultsSpec)

		// Cold storage (optional)
		namespaceName := cfg.Get("namespace")
		if namespaceName == "" {
			namespaceName = ctx.Stack()
		}

		var coldStorageDeps []pulumi.Resource
		if shared.GetConfigBool(cfg, "coldStorage-enabled", false) {
			coldStorageRegion := cfg.Get("coldStorage-s3-region")
			if coldStorageRegion == "" {
				coldStorageRegion = "eu-west-1"
			}

			awsProvider, awsErr := aws.NewProvider(ctx, "aws-cold-storage", &aws.ProviderArgs{
				Region: pulumi.String(coldStorageRegion),
			})
			if awsErr != nil {
				return fmt.Errorf("failed to create AWS provider for cold storage: %w", awsErr)
			}

			bucketName := fmt.Sprintf("ledger-exp-cold-storage-%s", ctx.Stack())
			coldBucket, bucketErr := s3.NewBucketV2(ctx, "cold-storage-bucket", &s3.BucketV2Args{
				Bucket: pulumi.String(bucketName),
			}, pulumi.Provider(awsProvider))
			if bucketErr != nil {
				return fmt.Errorf("failed to create cold storage S3 bucket: %w", bucketErr)
			}

			defaultsConfig, ok := defaultsSpec["config"].(map[string]any)
			if !ok {
				defaultsConfig = make(map[string]any)
				defaultsSpec["config"] = defaultsConfig
			}
			defaultsConfig["coldStorage"] = map[string]any{
				"driver": "s3",
				"s3": map[string]any{
					"bucket": coldBucket.Bucket,
					"region": coldStorageRegion,
				},
			}

			oidcIssuer := cfg.Get("coldStorage-eks-oidc-issuer")
			if oidcIssuer != "" {
				oidcIssuer = strings.TrimPrefix(oidcIssuer, "https://")

				callerIdentity, callerErr := aws.GetCallerIdentity(ctx, &aws.GetCallerIdentityArgs{}, pulumi.Provider(awsProvider))
				if callerErr != nil {
					return fmt.Errorf("failed to get AWS caller identity: %w", callerErr)
				}

				oidcProviderARN := fmt.Sprintf("arn:aws:iam::%s:oidc-provider/%s", callerIdentity.AccountId, oidcIssuer)
				trustPolicy, jsonErr := json.Marshal(map[string]any{
					"Version": "2012-10-17",
					"Statement": []map[string]any{{
						"Effect":    "Allow",
						"Principal": map[string]any{"Federated": oidcProviderARN},
						"Action":    "sts:AssumeRoleWithWebIdentity",
						"Condition": map[string]any{
							"StringEquals": map[string]string{
								oidcIssuer + ":sub": fmt.Sprintf("system:serviceaccount:%s:aws-access", namespaceName),
								oidcIssuer + ":aud": "sts.amazonaws.com",
							},
						},
					}},
				})
				if jsonErr != nil {
					return fmt.Errorf("failed to marshal trust policy: %w", jsonErr)
				}

				coldStorageRole, roleErr := iam.NewRole(ctx, "cold-storage-role", &iam.RoleArgs{
					Name:             pulumi.Sprintf("ledger-exp-%s-cold-storage", ctx.Stack()),
					AssumeRolePolicy: pulumi.String(trustPolicy),
				}, pulumi.Provider(awsProvider))
				if roleErr != nil {
					return fmt.Errorf("failed to create cold storage IAM role: %w", roleErr)
				}

				s3Policy, jsonErr := json.Marshal(map[string]any{
					"Version": "2012-10-17",
					"Statement": []map[string]any{{
						"Effect": "Allow",
						"Action": []string{
							"s3:GetObject",
							"s3:PutObject",
							"s3:DeleteObject",
							"s3:ListBucket",
						},
						"Resource": []string{
							fmt.Sprintf("arn:aws:s3:::%s", bucketName),
							fmt.Sprintf("arn:aws:s3:::%s/*", bucketName),
						},
					}},
				})
				if jsonErr != nil {
					return fmt.Errorf("failed to marshal S3 policy: %w", jsonErr)
				}

				_, policyErr := iam.NewRolePolicy(ctx, "cold-storage-s3-policy", &iam.RolePolicyArgs{
					Role:   coldStorageRole.Name,
					Policy: pulumi.String(s3Policy),
				}, pulumi.Provider(awsProvider))
				if policyErr != nil {
					return fmt.Errorf("failed to create cold storage S3 policy: %w", policyErr)
				}

				sa, saErr := v1.NewServiceAccount(ctx, "aws-access", &v1.ServiceAccountArgs{
					Metadata: &metav1.ObjectMetaArgs{
						Name:      pulumi.String("aws-access"),
						Namespace: namespace.Metadata.Name(),
						Annotations: pulumi.StringMap{
							"eks.amazonaws.com/role-arn": coldStorageRole.Arn,
						},
					},
				}, pulumi.Provider(k8sProvider))
				if saErr != nil {
					return fmt.Errorf("failed to create IRSA ServiceAccount: %w", saErr)
				}
				coldStorageDeps = append(coldStorageDeps, sa)
				ctx.Export("coldStorageRoleArn", coldStorageRole.Arn)
			}

			coldStorageDeps = append(coldStorageDeps, coldBucket)
			ctx.Export("coldStorageBucket", coldBucket.Bucket)
		}

		defaultsDeps := append([]pulumi.Resource{ledgerOperator}, ledgerCRDs...)
		defaultsDeps = append(defaultsDeps, coldStorageDeps...)

		_, err = apiextensions.NewCustomResource(ctx, "ledger-defaults", &apiextensions.CustomResourceArgs{
			ApiVersion: pulumi.String("ledger.formance.com/v1alpha1"),
			Kind:       pulumi.String("LedgerDefaults"),
			Metadata: &metav1.ObjectMetaArgs{
				Name: pulumi.String("default"),
				Annotations: pulumi.StringMap{
					"pulumi.com/patchForce": pulumi.String("true"),
				},
			},
			OtherFields: kubernetes.UntypedArgs{
				"spec": defaultsSpec,
			},
		},
			pulumi.DependsOn(defaultsDeps),
			pulumi.Provider(k8sProvider),
		)
		if err != nil {
			return fmt.Errorf("failed to create LedgerDefaults CR: %w", err)
		}

		// Exports
		ctx.Export("namespace", namespace.Metadata.Name())
		ctx.Export("dockerImage", pulumi.Sprintf("%s/formancehq/ledger-exp:latest@%s", dc.PullRegistry, dockerImage.Digest))
		ctx.Export("ledgerOperatorImage", pulumi.Sprintf("%s/formancehq/ledger-operator:latest@%s", dc.PullRegistry, ledgerOperatorImage.Digest))
		ctx.Export("ledgerOperatorRelease", ledgerOperator.Name)

		return nil
	})
}
