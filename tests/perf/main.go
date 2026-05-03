// Pulumi program: deploys a k6 Benchmark CR with its ConfigMap archive.
package main

import (
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apiextensions"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
)

func main() {
	pulumi.Run(func(ctx *pulumi.Context) error {
		cfg := config.New(ctx, "")

		kubeContext := cfg.Require("k8s-context")

		var benchmarkCfg map[string]any
		cfg.RequireObject("benchmark", &benchmarkCfg)

		scriptName, _ := benchmarkCfg["script"].(string)
		if scriptName == "" {
			return fmt.Errorf("benchmark.script is required")
		}

		metadata, _ := benchmarkCfg["metadata"].(map[string]any)
		namespaceName, _ := metadata["namespace"].(string)
		if namespaceName == "" {
			namespaceName = ctx.Stack()
		}

		benchmarkName, _ := metadata["name"].(string)
		benchmarkName = strings.ReplaceAll(benchmarkName, "_", "-")
		if benchmarkName == "" {
			benchmarkName = "k6-run-" + strings.ReplaceAll(scriptName, "_", "-")
		}

		// Create k6 archive
		archivePath, err := createK6Archive(scriptName)
		if err != nil {
			return fmt.Errorf("creating k6 archive: %w", err)
		}

		archiveBytes, err := os.ReadFile(archivePath)
		if err != nil {
			return fmt.Errorf("reading archive %s: %w", archivePath, err)
		}
		archiveBase64 := base64.StdEncoding.EncodeToString(archiveBytes)

		// K8s provider
		k8sProvider, err := kubernetes.NewProvider(ctx, "k8s", &kubernetes.ProviderArgs{
			Context: pulumi.StringPtr(kubeContext),
		})
		if err != nil {
			return fmt.Errorf("creating k8s provider: %w", err)
		}

		// ConfigMap with archive binary data
		configMapName := benchmarkName
		cm, err := corev1.NewConfigMap(ctx, "k6-archive", &corev1.ConfigMapArgs{
			Metadata: &metav1.ObjectMetaArgs{
				Name:      pulumi.String(configMapName),
				Namespace: pulumi.String(namespaceName),
			},
			BinaryData: pulumi.StringMap{
				scriptName + ".tar": pulumi.String(archiveBase64),
			},
		}, pulumi.Provider(k8sProvider))
		if err != nil {
			return fmt.Errorf("creating ConfigMap: %w", err)
		}

		// Benchmark CR
		_, err = apiextensions.NewCustomResource(ctx, "benchmark", &apiextensions.CustomResourceArgs{
			ApiVersion: pulumi.String("benchmark.formance.com/v1alpha1"),
			Kind:       pulumi.String("Benchmark"),
			Metadata: &metav1.ObjectMetaArgs{
				Name:      pulumi.String(benchmarkName),
				Namespace: pulumi.String(namespaceName),
			},
			OtherFields: buildBenchmarkSpec(benchmarkCfg, configMapName, scriptName),
		},
			pulumi.Provider(k8sProvider),
			pulumi.DependsOn([]pulumi.Resource{cm}),
		)
		if err != nil {
			return fmt.Errorf("creating Benchmark: %w", err)
		}

		ctx.Export("benchmarkName", pulumi.String(benchmarkName))
		ctx.Export("namespace", pulumi.String(namespaceName))
		ctx.Export("script", pulumi.String(scriptName))

		return nil
	})
}

func createK6Archive(scriptName string) (string, error) {
	scriptFile := scriptName + ".js"
	archiveFile := scriptName + ".tar"
	archivePath := filepath.Join("archives", archiveFile)

	_ = os.Remove(archivePath) // best-effort cleanup
	if err := os.MkdirAll("archives", 0o755); err != nil {
		return "", fmt.Errorf("creating archives directory: %w", err)
	}

	cmd := exec.Command("k6", "archive", scriptFile, "-O", filepath.Join("..", "archives", archiveFile))
	cmd.Dir = "scripts"
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("k6 archive failed: %w", err)
	}

	return archivePath, nil
}

func buildBenchmarkSpec(cfg map[string]any, configMapName, scriptName string) map[string]any {
	spec, _ := cfg["spec"].(map[string]any)
	if spec == nil {
		spec = map[string]any{"parallelism": 1}
	}

	// Build runner env as K8s env var array
	runner, _ := cfg["runner"].(map[string]any)
	runnerSpec := map[string]any{}

	if runner != nil {
		if envMap, ok := runner["env"].(map[string]any); ok {
			envVars := make([]map[string]any, 0, len(envMap))
			for k, v := range envMap {
				envVars = append(envVars, map[string]any{
					"name":  k,
					"value": fmt.Sprintf("%v", v),
				})
			}
			runnerSpec["env"] = envVars
		}
		if v, ok := runner["nodeSelector"]; ok {
			runnerSpec["nodeSelector"] = v
		}
		if v, ok := runner["tolerations"]; ok {
			runnerSpec["tolerations"] = v
		}
	}

	testRun := map[string]any{
		"parallelism": spec["parallelism"],
		"script": map[string]any{
			"configMap": map[string]any{
				"name": configMapName,
				"file": scriptName + ".tar",
			},
		},
		"runner": runnerSpec,
	}

	for _, key := range []string{"arguments", "separate", "cleanup"} {
		if v, ok := spec[key]; ok {
			testRun[key] = v
		}
	}

	result := map[string]any{
		"spec": map[string]any{
			"testRun": testRun,
		},
	}

	if resources, ok := cfg["resources"]; ok {
		result["spec"].(map[string]any)["resources"] = resources
	}

	return result
}
