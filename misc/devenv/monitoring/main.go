// Monitoring stack: VictoriaMetrics, Tempo, Loki, Pyroscope, OTLP, Grafana.
package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/deployments/devenv/shared"
	v1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
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

		configPath := "config"

		// Deploy monitoring backends sequentially to avoid Helm release storage conflicts.

		// VictoriaMetrics
		victoriaMetricsValues, err := shared.GetConfigObject(cfg, "victoriametrics", ".")
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

		// Tempo
		tempoValues, err := shared.GetConfigObject(cfg, "tempo", ".")
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

		// Loki
		lokiValues, err := shared.GetConfigObject(cfg, "loki", ".")
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

		// Pyroscope (optional)
		var pyroscope *helm.Release
		if shared.GetConfigBool(cfg, "pyroscope-enabled", true) {
			pyroscopeValues, pyroscopeErr := shared.GetConfigObject(cfg, "pyroscope", ".")
			if pyroscopeErr != nil {
				pyroscopeValues = make(map[string]any)
			}
			pyroscope, err = helm.NewRelease(ctx, "pyroscope", &helm.ReleaseArgs{
				Name:           pulumi.String("pyroscope"),
				Chart:          pulumi.String("pyroscope"),
				RepositoryOpts: &helm.RepositoryOptsArgs{Repo: pulumi.String("https://grafana.github.io/helm-charts")},
				Namespace:      namespace.Metadata.Name(),
				Values:         pulumi.ToMap(pyroscopeValues),
				ForceUpdate:    pulumi.Bool(true),
			},
				pulumi.DependsOn([]pulumi.Resource{namespace, loki}),
				pulumi.Provider(k8sProvider),
			)
			if err != nil {
				return fmt.Errorf("failed to deploy Pyroscope: %w", err)
			}
		}

		// OTLP
		otlpValues, err := shared.GetConfigObject(cfg, "otlp", ".")
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

		// Grafana dashboard ConfigMaps
		dashboards, err := shared.ReadDashboardFiles(configPath, "grafana/provisioning/dashboards")
		if err != nil {
			return fmt.Errorf("failed to read dashboard files: %w", err)
		}

		var grafanaDashboards []pulumi.Resource
		for _, dashboard := range dashboards {
			dashboardData := pulumi.StringMap{
				dashboard.Filename: pulumi.String(dashboard.Content),
			}
			grafanaDashboard, err := v1.NewConfigMap(ctx, fmt.Sprintf("grafana-dashboard-%s", dashboard.Name), &v1.ConfigMapArgs{
				Metadata: &metav1.ObjectMetaArgs{
					Name:      pulumi.String(fmt.Sprintf("grafana-dashboard-%s", dashboard.Name)),
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
				return fmt.Errorf("failed to create Grafana dashboard ConfigMap %s: %w", dashboard.Name, err)
			}
			grafanaDashboards = append(grafanaDashboards, grafanaDashboard)
		}

		// Dashboard provisioning ConfigMap
		dashboardYml, err := shared.ReadTextFile(configPath, "grafana/provisioning/dashboards/dashboard.yml")
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

		// Datasource provisioning ConfigMap
		datasourceVictoriaMetricsYml, err := shared.ReadTextFile(configPath, "grafana/provisioning/datasources/victoriametrics.yml")
		if err != nil {
			return fmt.Errorf("failed to read victoriametrics.yml: %w", err)
		}
		datasourcePyroscopeYml, err := shared.ReadTextFile(configPath, "grafana/provisioning/datasources/pyroscope.yml")
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
				"pyroscope.yml":      pulumi.String(datasourcePyroscopeYml),
			},
		},
			pulumi.DependsOn([]pulumi.Resource{namespace}),
			pulumi.Provider(k8sProvider),
		)
		if err != nil {
			return fmt.Errorf("failed to create Grafana datasource provisioning ConfigMap: %w", err)
		}

		// Grafana
		grafanaValues, err := shared.GetConfigObject(cfg, "grafana", ".")
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

		// Exports
		ctx.Export("namespace", namespace.Metadata.Name())
		ctx.Export("victoriaMetricsRelease", victoriaMetrics.Name)
		ctx.Export("tempoRelease", tempo.Name)
		ctx.Export("lokiRelease", loki.Name)
		ctx.Export("otlpRelease", otlp.Name)
		ctx.Export("grafanaRelease", grafana.Name)
		if pyroscope != nil {
			ctx.Export("pyroscopeRelease", pyroscope.Name)
		}

		return nil
	})
}
