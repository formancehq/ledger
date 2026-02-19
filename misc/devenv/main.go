package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/iam"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/s3"
	"github.com/pulumi/pulumi-docker-build/sdk/go/dockerbuild"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes"
	v1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
	"gopkg.in/yaml.v3"
)

// getBuildVersion generates a version string based on git commit and timestamp.
// Format: <short-commit>-<timestamp> (e.g., "abc1234-20260125-143022")
// If git is not available, falls back to timestamp only.
func getBuildVersion() string {
	// Get git short commit hash
	cmd := exec.Command("git", "rev-parse", "--short", "HEAD")
	cmd.Dir = "../.." // Root of the project
	output, err := cmd.Output()
	
	timestamp := time.Now().Format("20060102-150405")
	
	if err != nil {
		// Fallback to timestamp only
		return timestamp
	}
	
	commit := strings.TrimSpace(string(output))
	
	// Check if working directory is dirty
	cmd = exec.Command("git", "status", "--porcelain")
	cmd.Dir = "../.."
	statusOutput, _ := cmd.Output()
	
	if len(statusOutput) > 0 {
		// Working directory has uncommitted changes
		return fmt.Sprintf("%s-dirty-%s", commit, timestamp)
	}
	
	return fmt.Sprintf("%s-%s", commit, timestamp)
}

func main() {
	pulumi.Run(func(ctx *pulumi.Context) error {

		cfg := config.New(ctx, "")
		kubeContext := cfg.Require("k8s-context")

		// Get namespace from config, default to stack name
		namespaceName := cfg.Get("namespace")
		if namespaceName == "" {
			namespaceName = ctx.Stack()
		}

		k8sProvider, err := kubernetes.NewProvider(ctx, "k8s", &kubernetes.ProviderArgs{
			Context: pulumi.StringPtr(kubeContext),
		})
		if err != nil {
			return err
		}

		// Create a single namespace for all components
		namespace, err := v1.NewNamespace(ctx, "namespace", &v1.NamespaceArgs{
			Metadata: &metav1.ObjectMetaArgs{
				Name: pulumi.String(namespaceName),
			},
		}, pulumi.Provider(k8sProvider))
		if err != nil {
			return fmt.Errorf("failed to create namespace: %w", err)
		}

		// Helper function to read config objects from Pulumi config or YAML files
		getConfigObject := func(key string) (map[string]interface{}, error) {
			// First, try to get the config object
			var configObj map[string]interface{}
			if err := cfg.GetObject(key, &configObj); err != nil {
				return nil, fmt.Errorf("failed to get config object %s: %w", key, err)
			}

			// Check if the config contains a "file" key pointing to a YAML file
			if filePath, ok := configObj["file"].(string); ok {
				// Read the YAML file
				// The file path is relative to the devenv directory
				fullPath := filepath.Join(".", filePath)
				data, err := os.ReadFile(fullPath)
				if err != nil {
					return nil, fmt.Errorf("failed to read values file %s: %w", fullPath, err)
				}

				var result map[string]interface{}
				if err := yaml.Unmarshal(data, &result); err != nil {
					return nil, fmt.Errorf("failed to parse YAML file %s: %w", fullPath, err)
				}

				return result, nil
			}

			// If no "file" key, return the config object as-is (backward compatibility)
			return configObj, nil
		}
		getConfigBool := func(key string, fallback bool) bool {
			value := cfg.GetBool(key)
			if value {
				return true
			}
			// Check if the key exists but is explicitly set to false
			if cfg.Get(key) == "false" {
				return false
			}
			return fallback
		}

		// Build Docker images
		// Get registry from config or use default
		registry := cfg.Get("registry")
		if registry == "" {
			registry = "ghcr.io"
		}
		pullRegistry := cfg.Get("pull-registry")
		if pullRegistry == "" {
			pullRegistry = registry
		}
		
		// Generate build version from git commit + timestamp
		buildVersion := getBuildVersion()
		ctx.Log.Info(fmt.Sprintf("Build version: %s", buildVersion), nil)
		
		imageTag := cfg.Get("imageTag")
		if imageTag == "" {
			imageTag = buildVersion
		}

		// Build Docker image using the same parameters as justfile
		dockerImage, err := dockerbuild.NewImage(ctx, "formancehq/ledger-exp", &dockerbuild.ImageArgs{
			Context: dockerbuild.BuildContextArgs{
				Location: pulumi.String("../.."),
			},
			Builder: dockerbuild.BuilderConfigArgs{
				Name: pulumi.String("formance-runner"), // todo: make configurable
			},
			CacheFrom: dockerbuild.CacheFromArray{
				dockerbuild.CacheFromArgs{
					Registry: dockerbuild.CacheFromRegistryArgs{
						Ref: pulumi.Sprintf("%s/formancehq/ledger-exp:buildcache", registry),
					},
				},
			},
			CacheTo: dockerbuild.CacheToArray{
				dockerbuild.CacheToArgs{
					Registry: dockerbuild.CacheToRegistryArgs{
						Ref: pulumi.Sprintf("%s/formancehq/ledger-exp:buildcache,mode=max", registry),
					},
				},
			},
			Dockerfile: dockerbuild.DockerfileArgs{
				Location: pulumi.String("../../Dockerfile"),
			},
			Platforms: dockerbuild.PlatformArray{
				"linux/amd64",
			},
			Push: pulumi.Bool(true),
			Registries: dockerbuild.RegistryArray{
				dockerbuild.RegistryArgs{
					Address:  pulumi.String(registry),
					Username: config.GetSecret(ctx, "formance-dev-registry-username"),
					Password: config.GetSecret(ctx, "formance-dev-registry-password"),
				},
			},
		Tags: pulumi.StringArray{
			pulumi.Sprintf("%s/formancehq/ledger-exp:latest", registry),
		},
		})
		if err != nil {
			return fmt.Errorf("failed to build Docker image: %w", err)
		}

		benchmarkOperatorImage, err := dockerbuild.NewImage(ctx, "formancehq/benchmark-operator", &dockerbuild.ImageArgs{
			Context: dockerbuild.BuildContextArgs{
				Location: pulumi.String("../benchmark-operator"),
			},
			Builder: dockerbuild.BuilderConfigArgs{
				Name: pulumi.String("formance-runner"), // todo: make configurable
			},
			CacheFrom: dockerbuild.CacheFromArray{
				dockerbuild.CacheFromArgs{
					Registry: dockerbuild.CacheFromRegistryArgs{
						Ref: pulumi.Sprintf("%s/formancehq/benchmark-operator:buildcache", registry),
					},
				},
			},
			CacheTo: dockerbuild.CacheToArray{
				dockerbuild.CacheToArgs{
					Registry: dockerbuild.CacheToRegistryArgs{
						Ref: pulumi.Sprintf("%s/formancehq/benchmark-operator:buildcache,mode=max", registry),
					},
				},
			},
			Dockerfile: dockerbuild.DockerfileArgs{
				Location: pulumi.String("../benchmark-operator/Dockerfile"),
			},
			Platforms: dockerbuild.PlatformArray{
				"linux/amd64",
			},
			Push: pulumi.Bool(true),
			Registries: dockerbuild.RegistryArray{
				dockerbuild.RegistryArgs{
					Address:  pulumi.String(registry),
					Username: config.GetSecret(ctx, "formance-dev-registry-username"),
					Password: config.GetSecret(ctx, "formance-dev-registry-password"),
				},
			},
		Tags: pulumi.StringArray{
			pulumi.Sprintf("%s/formancehq/benchmark-operator:latest", registry),
		},
		})
		if err != nil {
			return fmt.Errorf("failed to build benchmark operator image: %w", err)
		}

		// Get the config directory for Grafana provisioning files (still needed for dashboards and datasources)
		k8sConfigPath := filepath.Join("config")

		// Helper function to read all dashboard JSON files from a directory
		readDashboardFiles := func(dirPath string) ([]struct {
			name     string
			filename string
			content  string
		}, error) {
			fullPath := filepath.Join(k8sConfigPath, dirPath)
			entries, err := os.ReadDir(fullPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read dashboard directory %s: %w", fullPath, err)
			}

			var dashboards []struct {
				name     string
				filename string
				content  string
			}

			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				// Only process JSON files, skip YAML files
				if filepath.Ext(entry.Name()) != ".json" {
					continue
				}

				filePath := filepath.Join(fullPath, entry.Name())
				data, err := os.ReadFile(filePath)
				if err != nil {
					return nil, fmt.Errorf("failed to read dashboard file %s: %w", filePath, err)
				}

				// Validate JSON by parsing it
				var jsonData map[string]interface{}
				if err := json.Unmarshal(data, &jsonData); err != nil {
					return nil, fmt.Errorf("failed to parse JSON file %s: %w", filePath, err)
				}

				// Marshal back to JSON with indentation
				jsonBytes, err := json.MarshalIndent(jsonData, "", "  ")
				if err != nil {
					return nil, fmt.Errorf("failed to marshal JSON file %s: %w", filePath, err)
				}

				// Generate a safe name for the ConfigMap (remove .json extension and sanitize)
				baseName := entry.Name()
				configMapName := baseName[:len(baseName)-len(filepath.Ext(baseName))]
				// Replace dots and other special chars with hyphens for Kubernetes resource names
				configMapName = filepath.Base(configMapName)

				dashboards = append(dashboards, struct {
					name     string
					filename string
					content  string
				}{
					name:     configMapName,
					filename: entry.Name(),
					content:  string(jsonBytes),
				})
			}

			return dashboards, nil
		}

		// Helper function to read text files (still needed for provisioning YAML files)
		readTextFile := func(filePath string) (string, error) {
			fullPath := filepath.Join(k8sConfigPath, filePath)
			data, err := os.ReadFile(fullPath)
			if err != nil {
				return "", fmt.Errorf("failed to read text file %s: %w", fullPath, err)
			}
			return string(data), nil
		}

		// Deploy monitoring backends sequentially to avoid Helm release storage conflicts
		// (concurrent helm install in the same namespace causes Secret race conditions)

		// Deploy VictoriaMetrics
		victoriaMetricsValues, err := getConfigObject("victoriametrics")
		if err != nil {
			return fmt.Errorf("failed to read VictoriaMetrics values: %w", err)
		}
		victoriaMetrics, err := helm.NewRelease(ctx, "victoriametrics", &helm.ReleaseArgs{
			Name:           pulumi.String("vm"),
			Chart:          pulumi.String("victoria-metrics-single"),
			RepositoryOpts: &helm.RepositoryOptsArgs{Repo: pulumi.String("https://victoriametrics.github.io/helm-charts/")},
			Namespace:      namespace.Metadata.Name(),
			Values:         pulumi.ToMap(victoriaMetricsValues),
			ForceUpdate:    pulumi.Bool(true),
		},
			pulumi.DependsOn([]pulumi.Resource{namespace}),
			pulumi.Provider(k8sProvider),
		)
		if err != nil {
			return fmt.Errorf("failed to deploy VictoriaMetrics: %w", err)
		}

		// Deploy Tempo (after VictoriaMetrics)
		tempoValues, err := getConfigObject("tempo")
		if err != nil {
			return fmt.Errorf("failed to read Tempo values: %w", err)
		}
		tempo, err := helm.NewRelease(ctx, "tempo", &helm.ReleaseArgs{
			Name:           pulumi.String("tempo"),
			Chart:          pulumi.String("tempo"),
			RepositoryOpts: &helm.RepositoryOptsArgs{Repo: pulumi.String("https://grafana.github.io/helm-charts")},
			Namespace:      namespace.Metadata.Name(),
			Values:         pulumi.ToMap(tempoValues),
			ForceUpdate:    pulumi.Bool(true),
		},
			pulumi.DependsOn([]pulumi.Resource{namespace, victoriaMetrics}),
			pulumi.Provider(k8sProvider),
		)
		if err != nil {
			return fmt.Errorf("failed to deploy Tempo: %w", err)
		}

		// Deploy Loki (after Tempo)
		lokiValues, err := getConfigObject("loki")
		if err != nil {
			return fmt.Errorf("failed to read Loki values: %w", err)
		}
		loki, err := helm.NewRelease(ctx, "loki", &helm.ReleaseArgs{
			Name:           pulumi.String("loki"),
			Chart:          pulumi.String("loki"),
			RepositoryOpts: &helm.RepositoryOptsArgs{Repo: pulumi.String("https://grafana.github.io/helm-charts")},
			Namespace:      namespace.Metadata.Name(),
			Values:         pulumi.ToMap(lokiValues),
			ForceUpdate:    pulumi.Bool(true),
		},
			pulumi.DependsOn([]pulumi.Resource{namespace, tempo}),
			pulumi.Provider(k8sProvider),
		)
		if err != nil {
			return fmt.Errorf("failed to deploy Loki: %w", err)
		}

		// Deploy Pyroscope (optional, enabled by default, after Loki)
		var pyroscope *helm.Release
		if getConfigBool("pyroscope-enabled", true) {
			pyroscopeValues, err := getConfigObject("pyroscope")
			if err != nil {
				// Pyroscope can work with default values
				pyroscopeValues = make(map[string]interface{})
			}
			pyroscopeDeps := []pulumi.Resource{namespace, loki}
			pyroscope, err = helm.NewRelease(ctx, "pyroscope", &helm.ReleaseArgs{
				Name:           pulumi.String("pyroscope"),
				Chart:          pulumi.String("pyroscope"),
				RepositoryOpts: &helm.RepositoryOptsArgs{Repo: pulumi.String("https://grafana.github.io/helm-charts")},
				Namespace:      namespace.Metadata.Name(),
				Values:         pulumi.ToMap(pyroscopeValues),
				ForceUpdate:    pulumi.Bool(true),
			},
				pulumi.DependsOn(pyroscopeDeps),
				pulumi.Provider(k8sProvider),
			)
			if err != nil {
				return fmt.Errorf("failed to deploy Pyroscope: %w", err)
			}
		}

		// Deploy OpenTelemetry Collector (after Loki/Pyroscope — needs backends ready for exporters)
		otlpValues, err := getConfigObject("otlp")
		if err != nil {
			return fmt.Errorf("failed to read OTLP values: %w", err)
		}
		otlpDeps := []pulumi.Resource{namespace, victoriaMetrics, tempo, loki}
		if pyroscope != nil {
			otlpDeps = append(otlpDeps, pyroscope)
		}
		otlp, err := helm.NewRelease(ctx, "otel", &helm.ReleaseArgs{
			Name:           pulumi.String("otel"),
			Chart:          pulumi.String("opentelemetry-collector"),
			RepositoryOpts: &helm.RepositoryOptsArgs{Repo: pulumi.String("https://open-telemetry.github.io/opentelemetry-helm-charts")},
			Namespace:      namespace.Metadata.Name(),
			Values:         pulumi.ToMap(otlpValues),
			ForceUpdate:    pulumi.Bool(true),
		},
			pulumi.DependsOn(otlpDeps),
			pulumi.Provider(k8sProvider),
		)
		if err != nil {
			return fmt.Errorf("failed to deploy OpenTelemetry Collector: %w", err)
		}

		// Create Grafana dashboard ConfigMaps
		// The Grafana sidecar will automatically discover ConfigMaps with the label grafana_dashboard="1"
		dashboards, err := readDashboardFiles("grafana/provisioning/dashboards")
		if err != nil {
			return fmt.Errorf("failed to read dashboard files: %w", err)
		}

		var grafanaDashboards []pulumi.Resource
		for _, dashboard := range dashboards {
			dashboardData := pulumi.StringMap{
				dashboard.filename: pulumi.String(dashboard.content),
			}
			grafanaDashboard, err := v1.NewConfigMap(ctx, fmt.Sprintf("grafana-dashboard-%s", dashboard.name), &v1.ConfigMapArgs{
				Metadata: &metav1.ObjectMetaArgs{
					Name:      pulumi.String(fmt.Sprintf("grafana-dashboard-%s", dashboard.name)),
					Namespace: namespace.Metadata.Name(),
					Labels: pulumi.StringMap{
						"grafana_dashboard": pulumi.String("1"),
					},
				},
				Data: dashboardData,
			},
				pulumi.DependsOn([]pulumi.Resource{namespace}),
				pulumi.Provider(k8sProvider),
			)
			if err != nil {
				return fmt.Errorf("failed to create Grafana dashboard ConfigMap %s: %w", dashboard.name, err)
			}
			grafanaDashboards = append(grafanaDashboards, grafanaDashboard)
		}

		// Create Grafana dashboard provisioning ConfigMap
		// This ConfigMap configures how Grafana discovers dashboards
		dashboardYml, err := readTextFile("grafana/provisioning/dashboards/dashboard.yml")
		if err != nil {
			return fmt.Errorf("failed to read dashboard.yml: %w", err)
		}
		grafanaDashboardProvisioning, err := v1.NewConfigMap(ctx, "grafana-dashboard-provisioning", &v1.ConfigMapArgs{
			Metadata: &metav1.ObjectMetaArgs{
				Name:      pulumi.String("grafana-dashboard-provisioning"),
				Namespace: namespace.Metadata.Name(),
			},
			Data: pulumi.StringMap{
				"dashboard.yml": pulumi.String(dashboardYml),
			},
		},
			pulumi.DependsOn([]pulumi.Resource{namespace}),
			pulumi.Provider(k8sProvider),
		)
		if err != nil {
			return fmt.Errorf("failed to create Grafana dashboard provisioning ConfigMap: %w", err)
		}

		// Create Grafana datasource provisioning ConfigMap
		// This ConfigMap configures the datasources in Grafana (VictoriaMetrics, Pyroscope)
		datasourceVictoriaMetricsYml, err := readTextFile("grafana/provisioning/datasources/victoriametrics.yml")
		if err != nil {
			return fmt.Errorf("failed to read victoriametrics.yml: %w", err)
		}
		datasourcePyroscopeYml, err := readTextFile("grafana/provisioning/datasources/pyroscope.yml")
		if err != nil {
			return fmt.Errorf("failed to read pyroscope.yml: %w", err)
		}
		grafanaDatasourceProvisioning, err := v1.NewConfigMap(ctx, "grafana-datasource-provisioning", &v1.ConfigMapArgs{
			Metadata: &metav1.ObjectMetaArgs{
				Name:      pulumi.String("grafana-datasource-provisioning"),
				Namespace: namespace.Metadata.Name(),
			},
			Data: pulumi.StringMap{
				"victoriametrics.yml": pulumi.String(datasourceVictoriaMetricsYml),
				"pyroscope.yml":       pulumi.String(datasourcePyroscopeYml),
			},
		},
			pulumi.DependsOn([]pulumi.Resource{namespace}),
			pulumi.Provider(k8sProvider),
		)
		if err != nil {
			return fmt.Errorf("failed to create Grafana datasource provisioning ConfigMap: %w", err)
		}

		// Deploy Grafana
		grafanaValues, err := getConfigObject("grafana")
		if err != nil {
			return fmt.Errorf("failed to read Grafana values: %w", err)
		}
		grafanaDeps := append([]pulumi.Resource{
			namespace,
			victoriaMetrics,
			tempo,
			loki,
			grafanaDashboardProvisioning,
			grafanaDatasourceProvisioning,
		}, grafanaDashboards...)
		if pyroscope != nil {
			grafanaDeps = append(grafanaDeps, pyroscope)
		}
		grafana, err := helm.NewRelease(ctx, "grafana", &helm.ReleaseArgs{
			Name:           pulumi.String("grafana"),
			Chart:          pulumi.String("grafana"),
			RepositoryOpts: &helm.RepositoryOptsArgs{Repo: pulumi.String("https://grafana.github.io/helm-charts")},
			Namespace:      namespace.Metadata.Name(),
			Values:         pulumi.ToMap(grafanaValues),
			ForceUpdate:    pulumi.Bool(true),
		},
			pulumi.DependsOn(grafanaDeps),
			pulumi.Provider(k8sProvider),
		)
		if err != nil {
			return fmt.Errorf("failed to deploy Grafana: %w", err)
		}

		// Deploy Ledger
		ledgerValues, err := getConfigObject("ledger")
		if err != nil {
			return fmt.Errorf("failed to read Ledger values: %w", err)
		}
		ledgerValues["image"] = map[string]any{
			"repository": pulumi.Sprintf("%s/formancehq/ledger-exp", pullRegistry),
			"tag":        pulumi.Sprintf("latest@%s", dockerImage.Digest),
		}
		
		// Add build version to Pyroscope tags for profile comparison
		if configMonitoring, ok := ledgerValues["config"].(map[string]interface{}); ok {
			if monitoring, ok := configMonitoring["monitoring"].(map[string]interface{}); ok {
				if pyroscope, ok := monitoring["pyroscope"].(map[string]interface{}); ok {
					existingTags := ""
					if tags, ok := pyroscope["tags"].(string); ok && tags != "" {
						existingTags = tags + ","
					}
					pyroscope["tags"] = fmt.Sprintf("%sversion=%s", existingTags, buildVersion)
				}
			}
		}
		// Cold storage: optionally create an S3 bucket and inject config into Helm values
		ledgerDeps := []pulumi.Resource{namespace, otlp, dockerImage}
		if getConfigBool("coldStorage-enabled", false) {
			coldStorageRegion := cfg.Get("coldStorage-s3-region")
			if coldStorageRegion == "" {
				coldStorageRegion = "eu-west-1"
			}

			awsProvider, err := aws.NewProvider(ctx, "aws-cold-storage", &aws.ProviderArgs{
				Region: pulumi.String(coldStorageRegion),
			})
			if err != nil {
				return fmt.Errorf("failed to create AWS provider for cold storage: %w", err)
			}

			bucketName := fmt.Sprintf("ledger-exp-cold-storage-%s", ctx.Stack())
			coldBucket, err := s3.NewBucketV2(ctx, "cold-storage-bucket", &s3.BucketV2Args{
				Bucket: pulumi.String(bucketName),
			}, pulumi.Provider(awsProvider))
			if err != nil {
				return fmt.Errorf("failed to create cold storage S3 bucket: %w", err)
			}

			// Ensure the config.coldStorage section exists in ledger values
			configMap, ok := ledgerValues["config"].(map[string]interface{})
			if !ok {
				configMap = make(map[string]interface{})
				ledgerValues["config"] = configMap
			}
			configMap["coldStorage"] = map[string]interface{}{
				"driver": "s3",
				"s3": map[string]interface{}{
					"bucket": coldBucket.Bucket,
					"region": coldStorageRegion,
				},
			}

			// Create IAM role and ServiceAccount for IRSA (IAM Roles for Service Accounts)
			oidcIssuer := cfg.Get("coldStorage-eks-oidc-issuer")
			if oidcIssuer != "" {
				// Strip https:// prefix if present
				oidcIssuer = strings.TrimPrefix(oidcIssuer, "https://")

				// Get AWS account ID
				callerIdentity, err := aws.GetCallerIdentity(ctx, &aws.GetCallerIdentityArgs{}, pulumi.Provider(awsProvider))
				if err != nil {
					return fmt.Errorf("failed to get AWS caller identity: %w", err)
				}

				// IAM role with OIDC trust policy scoped to this namespace/SA
				oidcProviderARN := fmt.Sprintf("arn:aws:iam::%s:oidc-provider/%s", callerIdentity.AccountId, oidcIssuer)
				trustPolicy, err := json.Marshal(map[string]any{
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
				if err != nil {
					return fmt.Errorf("failed to marshal trust policy: %w", err)
				}

				coldStorageRole, err := iam.NewRole(ctx, "cold-storage-role", &iam.RoleArgs{
					Name:             pulumi.Sprintf("ledger-exp-%s-cold-storage", ctx.Stack()),
					AssumeRolePolicy: pulumi.String(trustPolicy),
				}, pulumi.Provider(awsProvider))
				if err != nil {
					return fmt.Errorf("failed to create cold storage IAM role: %w", err)
				}

				// Inline S3 policy scoped to the cold storage bucket
				s3Policy, err := json.Marshal(map[string]any{
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
				if err != nil {
					return fmt.Errorf("failed to marshal S3 policy: %w", err)
				}

				_, err = iam.NewRolePolicy(ctx, "cold-storage-s3-policy", &iam.RolePolicyArgs{
					Role:   coldStorageRole.Name,
					Policy: pulumi.String(s3Policy),
				}, pulumi.Provider(awsProvider))
				if err != nil {
					return fmt.Errorf("failed to create cold storage S3 policy: %w", err)
				}

				// Create ServiceAccount annotated with the role ARN
				sa, err := v1.NewServiceAccount(ctx, "aws-access", &v1.ServiceAccountArgs{
					Metadata: &metav1.ObjectMetaArgs{
						Name:      pulumi.String("aws-access"),
						Namespace: namespace.Metadata.Name(),
						Annotations: pulumi.StringMap{
							"eks.amazonaws.com/role-arn": coldStorageRole.Arn,
						},
					},
				}, pulumi.Provider(k8sProvider))
				if err != nil {
					return fmt.Errorf("failed to create IRSA ServiceAccount: %w", err)
				}
				ledgerDeps = append(ledgerDeps, sa)
				ctx.Export("coldStorageRoleArn", coldStorageRole.Arn)
			}

			ledgerDeps = append(ledgerDeps, coldBucket)
			ctx.Export("coldStorageBucket", coldBucket.Bucket)
		}

		// Get the chart path (relative to the devenv directory where Pulumi.yaml is)
		// The chart is in ../chart relative to devenv
		chartPath := filepath.Join("..", "chart")
		ledger, err := helm.NewRelease(ctx, "ledger", &helm.ReleaseArgs{
			Name:             pulumi.String("ledger-exp"),
			Chart:            pulumi.String(chartPath),
			Namespace:        namespace.Metadata.Name(),
			Values:           pulumi.ToMap(ledgerValues),
			DependencyUpdate: pulumi.Bool(true),
			ForceUpdate:      pulumi.Bool(true),
		},
			pulumi.DependsOn(ledgerDeps),
			pulumi.Provider(k8sProvider),
		)
		if err != nil {
			return fmt.Errorf("failed to deploy Ledger: %w", err)
		}

		// Deploy k6-operator (optional, enabled by default)
		var k6Operator *helm.Release
		if getConfigBool("k6operator-enabled", true) {
			k6OperatorValues, err := getConfigObject("k6operator")
			if err != nil {
				// k6-operator can work with default values, so we use empty map if not configured
				k6OperatorValues = make(map[string]interface{})
			}
			k6Operator, err = helm.NewRelease(ctx, "k6-operator", &helm.ReleaseArgs{
				Name:           pulumi.String("k6-operator"),
				Chart:          pulumi.String("k6-operator"),
				RepositoryOpts: &helm.RepositoryOptsArgs{Repo: pulumi.String("https://grafana.github.io/helm-charts")},
				Namespace:      namespace.Metadata.Name(),
				Values:         pulumi.ToMap(k6OperatorValues),
				ForceUpdate:    pulumi.Bool(true),
			},
				pulumi.DependsOn([]pulumi.Resource{namespace, otlp}),
				pulumi.Provider(k8sProvider),
			)
			if err != nil {
				return fmt.Errorf("failed to deploy k6-operator: %w", err)
			}
			ctx.Export("k6OperatorRelease", k6Operator.Name)
		}

		// Deploy benchmark operator (optional, disabled by default)
		if getConfigBool("benchmarkOperator-enabled", false) {
			benchmarkOperatorValues, err := getConfigObject("benchmarkOperator")
			if err != nil {
				benchmarkOperatorValues = make(map[string]interface{})
			}
			var imageConfiguration map[string]any
			if benchmarkOperatorValues["image"] == nil {
				imageConfiguration = map[string]any{}
				benchmarkOperatorValues["image"] = imageConfiguration
			} else {
				imageConfiguration = benchmarkOperatorValues["image"].(map[string]any)
			}

			imageConfiguration["repository"] = pulumi.Sprintf("%s/formancehq/benchmark-operator", pullRegistry)
			imageConfiguration["tag"] = pulumi.Sprintf("latest@%s", benchmarkOperatorImage.Digest)

			benchmarkChartPath := filepath.Join("..", "benchmark-operator", "chart")

			benchmarkDeps := []pulumi.Resource{namespace, otlp, benchmarkOperatorImage}
			if k6Operator != nil {
				benchmarkDeps = append(benchmarkDeps, k6Operator)
			}

			benchmarkOperator, err := helm.NewRelease(ctx, "benchmark-operator", &helm.ReleaseArgs{
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
			if err != nil {
				return fmt.Errorf("failed to deploy benchmark operator: %w", err)
			}
			ctx.Export("benchmarkOperatorRelease", benchmarkOperator.Name)
		}

		// Export outputs
		ctx.Export("namespace", namespace.Metadata.Name())
		ctx.Export("dockerImage", dockerImage.Tags.Index(pulumi.Int(0)))
		ctx.Export("victoriaMetricsRelease", victoriaMetrics.Name)
		ctx.Export("tempoRelease", tempo.Name)
		ctx.Export("lokiRelease", loki.Name)
		ctx.Export("otlpRelease", otlp.Name)
		ctx.Export("grafanaRelease", grafana.Name)
		ctx.Export("ledgerRelease", ledger.Name)
		ctx.Export("benchmarkOperatorImage", benchmarkOperatorImage.Tags.Index(pulumi.Int(0)))
		if pyroscope != nil {
			ctx.Export("pyroscopeRelease", pyroscope.Name)
		}

		return nil
	})
}
