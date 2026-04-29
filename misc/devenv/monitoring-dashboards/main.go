package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apiextensions"
	v1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
)

func main() {
	pulumi.Run(func(ctx *pulumi.Context) error {
		cfg := config.New(ctx, "")

		kubeContext := cfg.Require("k8s-context")
		namespaceName := cfg.Get("namespace")
		if namespaceName == "" {
			namespaceName = ctx.Stack()
		}

		k8sProvider, err := kubernetes.NewProvider(ctx, "k8s", &kubernetes.ProviderArgs{
			Context: pulumi.StringPtr(kubeContext),
		})
		if err != nil {
			return fmt.Errorf("failed to create k8s provider: %w", err)
		}

		namespace, err := v1.NewNamespace(ctx, "namespace", &v1.NamespaceArgs{
			Metadata: &metav1.ObjectMetaArgs{
				Name: pulumi.String(namespaceName),
			},
		}, pulumi.Provider(k8sProvider))
		if err != nil {
			return fmt.Errorf("failed to create namespace: %w", err)
		}

		instanceSelector := cfg.Get("grafana-instance-selector")
		if instanceSelector == "" {
			instanceSelector = "grafana"
		}

		dashboards, err := readDashboardFiles("config/dashboards")
		if err != nil {
			return fmt.Errorf("failed to read dashboard files: %w", err)
		}

		for _, db := range dashboards {
			_, err = apiextensions.NewCustomResource(ctx, fmt.Sprintf("dashboard-%s", db.name), &apiextensions.CustomResourceArgs{
				ApiVersion: pulumi.String("grafana.integreatly.org/v1beta1"),
				Kind:       pulumi.String("GrafanaDashboard"),
				Metadata: &metav1.ObjectMetaArgs{
					Name:      pulumi.String(fmt.Sprintf("dashboard-%s", db.name)),
					Namespace: namespace.Metadata.Name(),
				},
				OtherFields: map[string]any{
					"spec": map[string]any{
						"instanceSelector": map[string]any{
							"matchLabels": map[string]any{
								"dashboards": instanceSelector,
							},
						},
						"json": db.content,
					},
				},
			},
				pulumi.DependsOn([]pulumi.Resource{namespace}),
				pulumi.Provider(k8sProvider),
			)
			if err != nil {
				return fmt.Errorf("failed to create GrafanaDashboard %s: %w", db.name, err)
			}
		}

		return nil
	})
}

type dashboard struct {
	name    string
	content string
}

func readDashboardFiles(dir string) ([]dashboard, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading directory %s: %w", dir, err)
	}

	var dashboards []dashboard

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", entry.Name(), err)
		}

		var parsed map[string]any
		if err := json.Unmarshal(data, &parsed); err != nil {
			return nil, fmt.Errorf("parsing %s: %w", entry.Name(), err)
		}

		jsonBytes, err := json.MarshalIndent(parsed, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("marshaling %s: %w", entry.Name(), err)
		}

		name := entry.Name()[:len(entry.Name())-len(filepath.Ext(entry.Name()))]

		dashboards = append(dashboards, dashboard{
			name:    name,
			content: string(jsonBytes),
		})
	}

	return dashboards, nil
}
