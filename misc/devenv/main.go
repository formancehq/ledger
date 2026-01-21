package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pulumi/pulumi-docker-build/sdk/go/dockerbuild"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes"
	v1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
	"gopkg.in/yaml.v3"
)

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
		imageTag := cfg.Get("imageTag")
		if imageTag == "" {
			imageTag = "latest"
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
				pulumi.Sprintf("%s/formancehq/benchmark-operator:%s", registry, imageTag),
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

		// Deploy Tempo
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
			pulumi.DependsOn([]pulumi.Resource{namespace}),
			pulumi.Provider(k8sProvider),
		)
		if err != nil {
			return fmt.Errorf("failed to deploy Tempo: %w", err)
		}

		// Deploy Loki
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
			pulumi.DependsOn([]pulumi.Resource{namespace}),
			pulumi.Provider(k8sProvider),
		)
		if err != nil {
			return fmt.Errorf("failed to deploy Loki: %w", err)
		}

		// Deploy OpenTelemetry Collector
		otlpValues, err := getConfigObject("otlp")
		if err != nil {
			return fmt.Errorf("failed to read OTLP values: %w", err)
		}
		otlp, err := helm.NewRelease(ctx, "otel", &helm.ReleaseArgs{
			Name:           pulumi.String("otel"),
			Chart:          pulumi.String("opentelemetry-collector"),
			RepositoryOpts: &helm.RepositoryOptsArgs{Repo: pulumi.String("https://open-telemetry.github.io/opentelemetry-helm-charts")},
			Namespace:      namespace.Metadata.Name(),
			Values:         pulumi.ToMap(otlpValues),
			ForceUpdate:    pulumi.Bool(true),
		},
			pulumi.DependsOn([]pulumi.Resource{namespace}),
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
		// This ConfigMap configures the VictoriaMetrics datasource in Grafana
		datasourceYml, err := readTextFile("grafana/provisioning/datasources/victoriametrics.yml")
		if err != nil {
			return fmt.Errorf("failed to read datasource.yml: %w", err)
		}
		grafanaDatasourceProvisioning, err := v1.NewConfigMap(ctx, "grafana-datasource-provisioning", &v1.ConfigMapArgs{
			Metadata: &metav1.ObjectMetaArgs{
				Name:      pulumi.String("grafana-datasource-provisioning"),
				Namespace: namespace.Metadata.Name(),
			},
			Data: pulumi.StringMap{
				"victoriametrics.yml": pulumi.String(datasourceYml),
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
		grafana, err := helm.NewRelease(ctx, "grafana", &helm.ReleaseArgs{
			Name:           pulumi.String("grafana"),
			Chart:          pulumi.String("grafana"),
			RepositoryOpts: &helm.RepositoryOptsArgs{Repo: pulumi.String("https://grafana.github.io/helm-charts")},
			Namespace:      namespace.Metadata.Name(),
			Values:         pulumi.ToMap(grafanaValues),
			ForceUpdate:    pulumi.Bool(true),
		},
			pulumi.DependsOn(append([]pulumi.Resource{
				namespace,
				victoriaMetrics,
				tempo,
				loki,
				grafanaDashboardProvisioning,
				grafanaDatasourceProvisioning,
			}, grafanaDashboards...)),
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
			pulumi.DependsOn([]pulumi.Resource{
				namespace,
				otlp,
				dockerImage,
			}),
			pulumi.Provider(k8sProvider),
		)
		if err != nil {
			return fmt.Errorf("failed to deploy Ledger: %w", err)
		}

		// Deploy k6-operator (optional, enabled by default)
		if getConfigBool("k6operator-enabled", true) {
			k6OperatorValues, err := getConfigObject("k6operator")
			if err != nil {
				// k6-operator can work with default values, so we use empty map if not configured
				k6OperatorValues = make(map[string]interface{})
			}
			k6Operator, err := helm.NewRelease(ctx, "k6-operator", &helm.ReleaseArgs{
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
			imageConfiguration["tag"] = pulumi.Sprintf("%s@%s", imageTag, benchmarkOperatorImage.Digest)

			benchmarkChartPath := filepath.Join("..", "benchmark-operator", "chart")

			benchmarkOperator, err := helm.NewRelease(ctx, "benchmark-operator", &helm.ReleaseArgs{
				Name:             pulumi.String("benchmark-operator"),
				Chart:            pulumi.String(benchmarkChartPath),
				Namespace:        namespace.Metadata.Name(),
				Values:           pulumi.ToMap(benchmarkOperatorValues),
				DependencyUpdate: pulumi.Bool(true),
				ForceUpdate:      pulumi.Bool(true),
			},
				pulumi.DependsOn([]pulumi.Resource{namespace, benchmarkOperatorImage}),
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

		return nil
	})
}
