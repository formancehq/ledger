// Devenv stack: builds ledger image, creates LedgerDefaults CR, provisions cold storage.
// The operator itself is deployed by the separate stack at misc/operator/deployment/pulumi/.
package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/iam"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/s3"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apiextensions"
	v1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"

	"github.com/formancehq/ledger-v3-poc/deployments/devenv/shared"
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

		// Build ledger Docker image
		dockerImage, err := dc.BuildImage(ctx, "formancehq/ledger-exp", "../../..", "../../../Dockerfile",
			shared.WithBuildArgs(pulumi.StringMap{
				"BUILD_TAGS": pulumi.String("kafka,nats,clickhouse,s3,pyroscope"),
			}),
		)
		if err != nil {
			return fmt.Errorf("failed to build Docker image: %w", err)
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

			// When pre-existing resources are provided, skip creation and reference them directly.
			existingBucket := cfg.Get("coldStorage-s3-bucket")
			existingRoleARN := cfg.Get("coldStorage-iam-role-arn")

			var bucketName pulumi.StringInput

			if existingBucket != "" {
				// Use pre-existing S3 bucket — admin-managed.
				bucketName = pulumi.String(existingBucket)
				ctx.Export("coldStorageBucket", pulumi.String(existingBucket))
			} else {
				// Create a new S3 bucket.
				awsProvider, awsErr := aws.NewProvider(ctx, "aws-cold-storage", &aws.ProviderArgs{
					Region: pulumi.String(coldStorageRegion),
				})
				if awsErr != nil {
					return fmt.Errorf("failed to create AWS provider for cold storage: %w", awsErr)
				}

				name := fmt.Sprintf("ledger-exp-cold-storage-%s", ctx.Stack())
				coldBucket, bucketErr := s3.NewBucketV2(ctx, "cold-storage-bucket", &s3.BucketV2Args{
					Bucket: pulumi.String(name),
				}, pulumi.Provider(awsProvider))
				if bucketErr != nil {
					return fmt.Errorf("failed to create cold storage S3 bucket: %w", bucketErr)
				}

				bucketName = coldBucket.Bucket
				coldStorageDeps = append(coldStorageDeps, coldBucket)
				ctx.Export("coldStorageBucket", coldBucket.Bucket)
			}

			defaultsConfig, ok := defaultsSpec["config"].(map[string]any)
			if !ok {
				defaultsConfig = make(map[string]any)
				defaultsSpec["config"] = defaultsConfig
			}
			defaultsConfig["coldStorage"] = map[string]any{
				"driver": "s3",
				"s3": map[string]any{
					"bucket": bucketName,
					"region": coldStorageRegion,
				},
			}

			// Configure pods to use the aws-access ServiceAccount (IRSA).
			createSA := false
			defaultsSpec["serviceAccount"] = map[string]any{
				"create": createSA,
				"name":   "aws-access",
			}

			if existingRoleARN != "" {
				// Use pre-existing IAM role — admin-managed.
				// Only create the K8s ServiceAccount with the IRSA annotation.
				sa, saErr := v1.NewServiceAccount(ctx, "aws-access", &v1.ServiceAccountArgs{
					Metadata: &metav1.ObjectMetaArgs{
						Name:      pulumi.String("aws-access"),
						Namespace: namespace.Metadata.Name(),
						Annotations: pulumi.StringMap{
							"eks.amazonaws.com/role-arn": pulumi.String(existingRoleARN),
						},
					},
				}, pulumi.Provider(k8sProvider))
				if saErr != nil {
					return fmt.Errorf("failed to create IRSA ServiceAccount: %w", saErr)
				}
				coldStorageDeps = append(coldStorageDeps, sa)
				ctx.Export("coldStorageRoleArn", pulumi.String(existingRoleARN))
			} else {
				// Create IAM role + policy + ServiceAccount via OIDC (IRSA).
				oidcIssuer := cfg.Get("coldStorage-eks-oidc-issuer")
				if oidcIssuer != "" {
					awsProvider, awsErr := aws.NewProvider(ctx, "aws-cold-storage-iam", &aws.ProviderArgs{
						Region: pulumi.String(coldStorageRegion),
					})
					if awsErr != nil {
						return fmt.Errorf("failed to create AWS provider for cold storage IAM: %w", awsErr)
					}

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

					s3BucketName := fmt.Sprintf("ledger-exp-cold-storage-%s", ctx.Stack())
					if existingBucket != "" {
						s3BucketName = existingBucket
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
								fmt.Sprintf("arn:aws:s3:::%s", s3BucketName),
								fmt.Sprintf("arn:aws:s3:::%s/*", s3BucketName),
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
			}
		}

		defaultsDeps := append([]pulumi.Resource{namespace}, coldStorageDeps...)

		_, err = apiextensions.NewCustomResource(ctx, "ledger-defaults-dev", &apiextensions.CustomResourceArgs{
			ApiVersion: pulumi.String("ledger.formance.com/v1alpha1"),
			Kind:       pulumi.String("LedgerDefaults"),
			Metadata: &metav1.ObjectMetaArgs{
				Name: pulumi.String("dev"),
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

		return nil
	})
}
