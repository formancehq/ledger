// Pulumi program: deploys a k6 Benchmark CR with its ConfigMap archive.
package main

import (
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apiextensions"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
)

func main() {
	pulumi.Run(runBenchmark)
}

func runBenchmark(ctx *pulumi.Context) error {
	cfg := config.New(ctx, "")

	kubeContext := cfg.Require("k8s-context")

	var benchmarkCfg map[string]any
	if profile := cfg.Get("profile"); profile != "" {
		var err error
		benchmarkCfg, err = ciBenchmark(profile, runIdentity{
			Commit: cfg.Require("commit"), Image: cfg.Require("image"),
			RunID: cfg.Require("run-id"), Attempt: cfg.Require("run-attempt"),
			PR:             cfg.Get("pr"),
			WhenBusy:       cfg.Get("when-busy"),
			WorkloadCommit: cfg.Get("workload-commit"),
		})
		if err != nil {
			return err
		}
	} else {
		cfg.RequireObject("benchmark", &benchmarkCfg)
	}

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
	defer func() { _ = os.RemoveAll(filepath.Dir(archivePath)) }() // Temporary build artifact.

	archiveBytes, err := os.ReadFile(archivePath)
	if err != nil {
		return fmt.Errorf("reading archive %s: %w", archivePath, err)
	}
	if len(archiveBytes) > 900*1024 {
		return fmt.Errorf("k6 archive is %d bytes; maximum is 900 KiB for a ConfigMap", len(archiveBytes))
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
	labels := pulumi.ToStringMap(benchmarkLabels(benchmarkCfg, benchmarkName))
	cm, err := corev1.NewConfigMap(ctx, "k6-archive", &corev1.ConfigMapArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Name:      pulumi.String(configMapName),
			Namespace: pulumi.String(namespaceName),
			Labels:    labels,
		},
		BinaryData: pulumi.StringMap{
			scriptName + ".tar": pulumi.String(archiveBase64),
		},
	}, pulumi.Provider(k8sProvider))
	if err != nil {
		return fmt.Errorf("creating ConfigMap: %w", err)
	}

	benchmarkSpec := buildBenchmarkSpec(benchmarkCfg, configMapName, scriptName)
	// The submission job ends at admission. Kubernetes owns the run lifecycle.
	_, err = apiextensions.NewCustomResource(ctx, "benchmark", &apiextensions.CustomResourceArgs{
		ApiVersion: pulumi.String("benchmark.formance.com/v1alpha1"),
		Kind:       pulumi.String("Benchmark"),
		Metadata: &metav1.ObjectMetaArgs{
			Name:        pulumi.String(benchmarkName),
			Namespace:   pulumi.String(namespaceName),
			Labels:      labels,
			Annotations: pulumi.ToStringMap(benchmarkAnnotations(benchmarkCfg)),
		},
		OtherFields: benchmarkSpec,
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
}

func createK6Archive(scriptName string) (string, error) {
	if !regexp.MustCompile(`^[a-z][a-z0-9_]*$`).MatchString(scriptName) {
		return "", fmt.Errorf("invalid script name %q", scriptName)
	}
	scriptFile := scriptName + ".js"
	archiveFile := scriptName + ".tar"
	archiveDir := filepath.Join("..", "..", "build", "perf", "archives")
	if err := os.MkdirAll(archiveDir, 0o755); err != nil {
		return "", fmt.Errorf("creating archives directory: %w", err)
	}
	tmp, err := os.MkdirTemp(archiveDir, "k6-")
	if err != nil {
		return "", fmt.Errorf("creating archive directory: %w", err)
	}
	archivePath, err := filepath.Abs(filepath.Join(tmp, archiveFile))
	if err != nil {
		return "", err
	}

	cmd := exec.Command("k6", "archive", "--include-system-env-vars=false", scriptFile, "-O", archivePath)
	cmd.Dir = "scripts"
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		_ = os.RemoveAll(tmp) // Best-effort cleanup of a failed archive build.
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
		for key, value := range runner {
			if key != "env" {
				runnerSpec[key] = value
			}
		}
		if envMap, ok := runner["env"].(map[string]any); ok {
			envVars := make([]map[string]any, 0, len(envMap))
			keys := make([]string, 0, len(envMap))
			for k := range envMap {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			for _, k := range keys {
				envVars = append(envVars, map[string]any{
					"name":  k,
					"value": fmt.Sprintf("%v", envMap[k]),
				})
			}
			runnerSpec["env"] = envVars
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

	for _, key := range []string{"arguments", "separate", "cleanup", "initializer", "starter"} {
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
	for _, key := range []string{"grafanaVariables", "executionGroup", "activeDeadlineSeconds", "whenBusy", "queueDeadlineSeconds", "cancel"} {
		if value, ok := cfg[key]; ok {
			result["spec"].(map[string]any)[key] = value
		}
	}

	return result
}
