package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pulumi/pulumi-docker-build/sdk/go/dockerbuild"
	v1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
)

func main() {
	pulumi.Run(func(ctx *pulumi.Context) error {

		// Create namespace
		monitoringNamespace, err := v1.NewNamespace(ctx, "monitoring", &v1.NamespaceArgs{
			Metadata: &metav1.ObjectMetaArgs{
				Name: pulumi.String("monitoring"),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create namespace: %w", err)
		}

		benchNamespace, err := v1.NewNamespace(ctx, "bench", &v1.NamespaceArgs{
			Metadata: &metav1.ObjectMetaArgs{
				Name: pulumi.String("bench"),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create namespace: %w", err)
		}

		ledgerNamespace, err := v1.NewNamespace(ctx, "ledger", &v1.NamespaceArgs{
			Metadata: &metav1.ObjectMetaArgs{
				Name: pulumi.String("ledger"),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create namespace: %w", err)
		}

		// Helper function to read config objects from Pulumi config
		cfg := config.New(ctx, "")
		getConfigObject := func(key string) (map[string]interface{}, error) {
			var result map[string]interface{}
			if err := cfg.GetObject(key, &result); err != nil {
				return nil, fmt.Errorf("failed to get config object %s: %w", key, err)
			}
			return result, nil
		}

		// Build Docker image
		// Get registry from config or use default
		registry := cfg.Get("registry")
		if registry == "" {
			registry = "ghcr.io"
		}
		imageTag := cfg.Get("imageTag")
		if imageTag == "" {
			imageTag = "latest"
		}

		// Get the project root directory (parent of devenv)
		// devenv is in misc/devenv, so we need to go up two levels to reach project root
		projectRoot, err := filepath.Abs("../..")
		if err != nil {
			return fmt.Errorf("failed to get project root: %w", err)
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
					Username: config.RequireSecret(ctx, "formance-dev-registry-username"),
					Password: config.RequireSecret(ctx, "formance-dev-registry-password"),
				},
			},
			Tags: pulumi.StringArray{
				pulumi.Sprintf("%s/formancehq/ledger-exp:latest", registry),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to build Docker image: %w", err)
		}

		// Get the config directory for Grafana provisioning files (still needed for dashboards and datasources)
		k8sConfigPath := filepath.Join("config")

		// Helper function to read JSON files (still needed for dashboard JSON)
		readJsonFile := func(filePath string) (map[string]interface{}, error) {
			fullPath := filepath.Join(k8sConfigPath, filePath)
			data, err := os.ReadFile(fullPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read JSON file %s: %w", fullPath, err)
			}
			var result map[string]interface{}
			if err := json.Unmarshal(data, &result); err != nil {
				return nil, fmt.Errorf("failed to parse JSON file %s: %w", fullPath, err)
			}
			return result, nil
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

		// Helper function to read directory and create a map of files
		readDirectoryFiles := func(dirPath string) (map[string]string, error) {
			// dirPath is relative to project root (e.g., "k6/scripts")
			fullPath := filepath.Join(projectRoot, dirPath)
			files := make(map[string]string)
			entries, err := os.ReadDir(fullPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read directory %s: %w", fullPath, err)
			}
			for _, entry := range entries {
				if !entry.IsDir() {
					filePath := filepath.Join(fullPath, entry.Name())
					data, err := os.ReadFile(filePath)
					if err != nil {
						return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
					}
					files[entry.Name()] = string(data)
				}
			}
			return files, nil
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
			Namespace:      monitoringNamespace.Metadata.Name(),
			Values:         pulumi.ToMap(victoriaMetricsValues),
		}, pulumi.DependsOn([]pulumi.Resource{monitoringNamespace}))
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
			Namespace:      monitoringNamespace.Metadata.Name(),
			Values:         pulumi.ToMap(tempoValues),
		}, pulumi.DependsOn([]pulumi.Resource{monitoringNamespace}))
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
			Namespace:      monitoringNamespace.Metadata.Name(),
			Values:         pulumi.ToMap(lokiValues),
		}, pulumi.DependsOn([]pulumi.Resource{monitoringNamespace}))
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
			Namespace:      monitoringNamespace.Metadata.Name(),
			Values:         pulumi.ToMap(otlpValues),
		}, pulumi.DependsOn([]pulumi.Resource{monitoringNamespace}))
		if err != nil {
			return fmt.Errorf("failed to deploy OpenTelemetry Collector: %w", err)
		}

		// Create Grafana dashboard ConfigMap
		// The Grafana sidecar will automatically discover ConfigMaps with the label grafana_dashboard="1"
		dashboardJson, err := readJsonFile("grafana/provisioning/dashboards/ledger-metrics.json")
		if err != nil {
			return fmt.Errorf("failed to read dashboard JSON: %w", err)
		}
		dashboardJsonBytes, err := json.MarshalIndent(dashboardJson, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal dashboard JSON: %w", err)
		}
		grafanaDashboard, err := v1.NewConfigMap(ctx, "grafana-dashboard", &v1.ConfigMapArgs{
			Metadata: &metav1.ObjectMetaArgs{
				Name:      pulumi.String("grafana-dashboard"),
				Namespace: monitoringNamespace.Metadata.Name(),
				Labels: pulumi.StringMap{
					"grafana_dashboard": pulumi.String("1"),
				},
			},
			Data: pulumi.StringMap{
				"ledger-metrics.json": pulumi.String(string(dashboardJsonBytes)),
			},
		}, pulumi.DependsOn([]pulumi.Resource{monitoringNamespace}))
		if err != nil {
			return fmt.Errorf("failed to create Grafana dashboard ConfigMap: %w", err)
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
				Namespace: monitoringNamespace.Metadata.Name(),
			},
			Data: pulumi.StringMap{
				"dashboard.yml": pulumi.String(dashboardYml),
			},
		}, pulumi.DependsOn([]pulumi.Resource{monitoringNamespace}))
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
				Namespace: monitoringNamespace.Metadata.Name(),
			},
			Data: pulumi.StringMap{
				"victoriametrics.yml": pulumi.String(datasourceYml),
			},
		}, pulumi.DependsOn([]pulumi.Resource{monitoringNamespace}))
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
			Namespace:      monitoringNamespace.Metadata.Name(),
			Values:         pulumi.ToMap(grafanaValues),
		}, pulumi.DependsOn([]pulumi.Resource{
			monitoringNamespace,
			victoriaMetrics,
			tempo,
			loki,
			grafanaDashboard,
			grafanaDashboardProvisioning,
			grafanaDatasourceProvisioning,
		}))
		if err != nil {
			return fmt.Errorf("failed to deploy Grafana: %w", err)
		}

		// Deploy Ledger
		ledgerValues, err := getConfigObject("ledger")
		if err != nil {
			return fmt.Errorf("failed to read Ledger values: %w", err)
		}
		ledgerValues["image"] = map[string]any{
			"repository": pulumi.Sprintf("%s/formancehq/ledger-exp", registry),
			"tag":        pulumi.Sprintf("latest@%s", dockerImage.Digest),
		}
		// Get the chart path (relative to the devenv directory where Pulumi.yaml is)
		// The chart is in ../chart relative to devenv
		chartPath := filepath.Join("..", "chart")
		ledger, err := helm.NewRelease(ctx, "ledger", &helm.ReleaseArgs{
			Name:             pulumi.String("ledger-exp"),
			Chart:            pulumi.String(chartPath),
			Namespace:        ledgerNamespace.Metadata.Name(),
			Values:           pulumi.ToMap(ledgerValues),
			DependencyUpdate: pulumi.Bool(true),
		}, pulumi.DependsOn([]pulumi.Resource{
			otlp,
			dockerImage,
		}))
		if err != nil {
			return fmt.Errorf("failed to deploy Ledger: %w", err)
		}

		// Deploy k6-operator
		k6OperatorValues, err := getConfigObject("k6operator")
		if err != nil {
			// k6-operator can work with default values, so we use empty map if not configured
			k6OperatorValues = make(map[string]interface{})
		}
		k6Operator, err := helm.NewRelease(ctx, "k6-operator", &helm.ReleaseArgs{
			Name:           pulumi.String("k6-operator"),
			Chart:          pulumi.String("k6-operator"),
			RepositoryOpts: &helm.RepositoryOptsArgs{Repo: pulumi.String("https://grafana.github.io/helm-charts")},
			Namespace:      benchNamespace.Metadata.Name(),
			Values:         pulumi.ToMap(k6OperatorValues),
		}, pulumi.DependsOn([]pulumi.Resource{monitoringNamespace}))
		if err != nil {
			return fmt.Errorf("failed to deploy k6-operator: %w", err)
		}

		// Create k6-scripts ConfigMap
		k6ScriptsFiles, err := readDirectoryFiles("misc/k6/scripts")
		if err != nil {
			return fmt.Errorf("failed to read k6 scripts directory: %w", err)
		}
		_, err = v1.NewConfigMap(ctx, "k6-scripts", &v1.ConfigMapArgs{
			Metadata: &metav1.ObjectMetaArgs{
				Name:      pulumi.String("k6-scripts"),
				Namespace: benchNamespace.Metadata.Name(),
			},
			Data: pulumi.ToStringMap(k6ScriptsFiles),
		}, pulumi.DependsOn([]pulumi.Resource{monitoringNamespace, k6Operator}))
		if err != nil {
			return fmt.Errorf("failed to create k6-scripts ConfigMap: %w", err)
		}

		// Export outputs
		ctx.Export("monitoringNamespace", monitoringNamespace.Metadata.Name())
		ctx.Export("dockerImage", dockerImage.Tags.Index(pulumi.Int(0)))
		ctx.Export("victoriaMetricsRelease", victoriaMetrics.Name)
		ctx.Export("tempoRelease", tempo.Name)
		ctx.Export("lokiRelease", loki.Name)
		ctx.Export("otlpRelease", otlp.Name)
		ctx.Export("grafanaRelease", grafana.Name)
		ctx.Export("ledgerRelease", ledger.Name)
		ctx.Export("k6OperatorRelease", k6Operator.Name)

		return nil
	})
}
