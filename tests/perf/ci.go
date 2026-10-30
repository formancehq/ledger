package main

import (
	_ "embed"
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

//go:embed profiles/acme.yaml
var acmeProfile []byte

type runIdentity struct {
	Commit         string
	Image          string
	RunID          string
	Attempt        string
	PR             string
	WhenBusy       string
	WorkloadCommit string
}

func ciBenchmark(profile string, run runIdentity) (map[string]any, error) {
	if !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(run.Commit) {
		return nil, fmt.Errorf("commit must be a full lowercase Git SHA")
	}
	number := regexp.MustCompile(`^[1-9][0-9]*$`)
	if !number.MatchString(run.RunID) || !number.MatchString(run.Attempt) {
		return nil, fmt.Errorf("run ID and attempt must be positive integers")
	}
	if run.PR != "" && run.PR != "0" && !number.MatchString(run.PR) {
		return nil, fmt.Errorf("PR must be zero or a positive integer")
	}
	if run.WhenBusy != "" && run.WhenBusy != "Queue" && run.WhenBusy != "Skip" {
		return nil, fmt.Errorf("when-busy must be Queue or Skip")
	}
	if run.WorkloadCommit != "" && !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(run.WorkloadCommit) {
		return nil, fmt.Errorf("workload-commit must be a full lowercase Git SHA")
	}
	// ImageSpec renders repository:tag. Keep the tag and digest together so
	// the disposable Cluster pulls exactly the image built for this revision.
	image := regexp.MustCompile(`^([a-z0-9./_-]+):([A-Za-z0-9_.-]+@sha256:[0-9a-f]{64})$`).FindStringSubmatch(run.Image)
	if image == nil {
		return nil, fmt.Errorf("image must use repository:tag@sha256:digest")
	}
	if strings.Contains(strings.Split(image[2], "@")[0], "-rc") {
		return nil, fmt.Errorf("CI benchmarks require a non-RC tag; RC upgrade identities must remain canonical")
	}
	var cfg map[string]any
	if err := yaml.Unmarshal(acmeProfile, &cfg); err != nil {
		return nil, fmt.Errorf("reading embedded acme profile: %w", err)
	}
	var stages string
	switch profile {
	case "smoke-5m":
		stages = "--stage 30s:400 --stage 4m:800 --stage 30s:0"
	case "smoke-10m":
		stages = "--stage 1m:400 --stage 8m:800 --stage 1m:0"
	case "soak-24h":
		stages = "--stage 1m:400 --stage 23h58m:800 --stage 1m:0"
	default:
		return nil, fmt.Errorf("unknown CI profile %q", profile)
	}
	name := "ledger-" + run.Commit[:12] + "-" + run.RunID + "-" + run.Attempt
	if len(name) > 48 { // Leave room for resource, StatefulSet and Pod suffixes.
		return nil, fmt.Errorf("run identity is too long for Kubernetes resource names")
	}
	metadata := cfg["metadata"].(map[string]any)
	metadata["name"] = name
	metadata["labels"] = map[string]string{
		"app.kubernetes.io/managed-by":   "ledger-perf",
		"benchmark.formance.com/profile": profile,
		"benchmark.formance.com/commit":  run.Commit,
		"benchmark.formance.com/pr":      run.PR,
	}
	metadata["annotations"] = map[string]string{
		"benchmark.formance.com/commit":      run.Commit,
		"benchmark.formance.com/image":       run.Image,
		"benchmark.formance.com/run-id":      run.RunID,
		"benchmark.formance.com/run-attempt": run.Attempt,
	}
	if run.WorkloadCommit != "" {
		metadata["annotations"].(map[string]string)["benchmark.formance.com/workload-commit"] = run.WorkloadCommit
	}
	clusterName := name + "-res-0"
	resources := cfg["resources"].([]any)
	cluster := resources[0].(map[string]any)["manifest"].(map[string]any)["spec"].(map[string]any)
	cluster["image"] = map[string]any{"repository": image[1], "tag": image[2]}
	resources[1].(map[string]any)["manifest"].(map[string]any)["spec"].(map[string]any)["clusterRef"] = clusterName
	cfg["grafanaVariables"] = map[string]any{"cluster": clusterName}
	cfg["runner"].(map[string]any)["env"].(map[string]any)["HTTP_ADDR"] = "http://ledger-" + clusterName + ":9000"
	cfg["spec"].(map[string]any)["arguments"] = "-o opentelemetry " + stages
	cfg["activeDeadlineSeconds"] = 7200
	cfg["queueDeadlineSeconds"] = 7200
	cfg["whenBusy"] = "Queue"
	if profile == "soak-24h" {
		cfg["activeDeadlineSeconds"] = 93600
		cfg["whenBusy"] = "Skip"
	}
	if run.WhenBusy != "" {
		cfg["whenBusy"] = run.WhenBusy
	}
	return cfg, nil
}

func benchmarkLabels(cfg map[string]any, name string) map[string]string {
	labels := metadataStrings(cfg, "labels")
	labels["benchmark.formance.com/name"] = name
	return labels
}

func benchmarkAnnotations(cfg map[string]any) map[string]string {
	annotations := metadataStrings(cfg, "annotations")
	annotations["pulumi.com/skipAwait"] = "true"
	return annotations
}

func metadataStrings(cfg map[string]any, key string) map[string]string {
	result := map[string]string{}
	if metadata, ok := cfg["metadata"].(map[string]any); ok {
		switch values := metadata[key].(type) {
		case map[string]string:
			for name, value := range values {
				result[name] = value
			}
		case map[string]any:
			for name, value := range values {
				result[name] = fmt.Sprint(value)
			}
		}
	}
	return result
}
