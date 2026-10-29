package main

import (
	"archive/tar"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func testIdentity() runIdentity {
	return runIdentity{Commit: strings.Repeat("a", 40), Image: "ghcr.io/formancehq/ledger:pr-42-aaaaaaa@sha256:" + strings.Repeat("b", 64), RunID: "123456", Attempt: "1", PR: "42"}
}

func TestCIProfiles(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		profile, stages, policy string
		deadline                int
	}{
		{"smoke-5m", "--stage 30s:400 --stage 4m:800 --stage 30s:0", "Queue", 7200},
		{"smoke-10m", "--stage 1m:400 --stage 8m:800 --stage 1m:0", "Queue", 7200},
		{"soak-24h", "--stage 1m:400 --stage 23h58m:800 --stage 1m:0", "Skip", 93600},
	} {
		t.Run(tt.profile, func(t *testing.T) {
			t.Parallel()
			run := testIdentity()
			cfg, err := ciBenchmark(tt.profile, run)
			if err != nil {
				t.Fatal(err)
			}
			name := "ledger-aaaaaaaaaaaa-123456-1"
			if cfg["metadata"].(map[string]any)["name"] != name {
				t.Fatal("incorrect run name")
			}
			spec := buildBenchmarkSpec(cfg, name, "world_to_bank")["spec"].(map[string]any)
			if spec["activeDeadlineSeconds"] != tt.deadline || spec["whenBusy"] != tt.policy || spec["executionGroup"] != "acme-ledger-m8a" {
				t.Fatal("missing lifecycle limits", spec)
			}
			if spec["grafanaVariables"].(map[string]any)["cluster"] != name+"-res-0" {
				t.Fatal("incorrect snapshot cluster")
			}
			testRun := spec["testRun"].(map[string]any)
			if testRun["arguments"] != "-o opentelemetry "+tt.stages {
				t.Fatal("incorrect load duration")
			}
			if _, present := testRun["cleanup"]; present {
				t.Fatal("runner must remain until snapshot and lifecycle cleanup")
			}
			runner := testRun["runner"].(map[string]any)
			if !strings.Contains(runner["image"].(string), "@sha256:") {
				t.Fatal("unpinned runner")
			}
			if runner["image"] != testRun["initializer"].(map[string]any)["image"] {
				t.Fatal("runtime version differs from initializer")
			}
			foundAddr := false
			for _, env := range runner["env"].([]map[string]any) {
				if env["name"] == "HTTP_ADDR" {
					foundAddr = env["value"] == "http://ledger-"+name+"-res-0:9000"
				}
			}
			if !foundAddr {
				t.Fatal("incorrect service address")
			}
			resources := spec["resources"].([]any)
			cluster := resources[0].(map[string]any)["manifest"].(map[string]any)["spec"].(map[string]any)
			image := cluster["image"].(map[string]any)
			if image["repository"].(string)+":"+image["tag"].(string) != run.Image {
				t.Fatal("image digest lost")
			}
			if cluster["replicas"] != 3 {
				t.Fatal("hardware drift")
			}
			monitoring := cluster["monitoring"].(map[string]any)
			for _, forbidden := range []string{"logs", "traces"} {
				if _, present := monitoring[forbidden]; present {
					t.Fatalf("%s exporter must not be configured", forbidden)
				}
			}
			ledger := resources[1].(map[string]any)["manifest"].(map[string]any)["spec"].(map[string]any)
			if ledger["clusterRef"] != name+"-res-0" {
				t.Fatal("incorrect cluster reference")
			}
			if benchmarkLabels(cfg, name)["benchmark.formance.com/name"] != name {
				t.Fatal("archive adoption label missing")
			}
			annotations := benchmarkAnnotations(cfg)
			if annotations["pulumi.com/skipAwait"] != "true" || annotations["benchmark.formance.com/commit"] != run.Commit {
				t.Fatal("missing asynchronous submission metadata")
			}
		})
	}
}

func TestIdentityValidationAndRetry(t *testing.T) {
	t.Parallel()
	for _, mutate := range []func(*runIdentity){
		func(r *runIdentity) { r.Commit = "short" },
		func(r *runIdentity) { r.RunID = "../stack" },
		func(r *runIdentity) { r.Attempt = "0" },
		func(r *runIdentity) { r.PR = "1\nkey=value" },
		func(r *runIdentity) { r.Image = "ghcr.io/formancehq/ledger:latest" },
		func(r *runIdentity) {
			r.Image = "ghcr.io/formancehq/ledger:v3.0.0-rc.1@sha256:" + strings.Repeat("b", 64)
		},
		func(r *runIdentity) { r.WhenBusy = "Ignore" },
	} {
		run := testIdentity()
		mutate(&run)
		if _, err := ciBenchmark("smoke-5m", run); err == nil {
			t.Fatalf("accepted invalid identity: %+v", run)
		}
	}
	if _, err := ciBenchmark("unknown", testIdentity()); err == nil {
		t.Fatal("accepted unknown profile")
	}
	first, err := ciBenchmark("smoke-5m", testIdentity())
	if err != nil {
		t.Fatal(err)
	}
	run := testIdentity()
	run.Attempt = "2"
	run.WhenBusy = "Queue"
	second, err := ciBenchmark("soak-24h", run)
	if err != nil {
		t.Fatal(err)
	}
	if first["metadata"].(map[string]any)["name"] == second["metadata"].(map[string]any)["name"] {
		t.Fatal("retry reused completed Benchmark")
	}
	if second["whenBusy"] != "Queue" {
		t.Fatal("manual queue override lost")
	}
}

func TestLegacySpecPreservesRunnerAndSnapshot(t *testing.T) {
	t.Parallel()
	cfg := map[string]any{"runner": map[string]any{"env": map[string]any{"Z": "last", "A": "first"}, "resources": map[string]any{"limits": map[string]any{"cpu": "4"}}}, "grafanaVariables": map[string]any{"cluster": "custom"}}
	first := buildBenchmarkSpec(cfg, "archive", "world_to_bank")
	second := buildBenchmarkSpec(cfg, "archive", "world_to_bank")
	if !reflect.DeepEqual(first, second) {
		t.Fatal("nondeterministic environment order")
	}
	spec := first["spec"].(map[string]any)
	if spec["testRun"].(map[string]any)["parallelism"] != 1 {
		t.Fatal("missing default parallelism")
	}
	if spec["grafanaVariables"].(map[string]any)["cluster"] != "custom" {
		t.Fatal("snapshot variable discarded")
	}
}

func TestArchiveIncludesSharedWithoutSystemEnvironment(t *testing.T) {
	t.Setenv("PERF_SECRET_SENTINEL", "must-never-enter-the-archive-59827")
	path, err := createK6Archive("world_to_bank")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(filepath.Dir(path)); err != nil {
			t.Error(err)
		}
	})
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader := tar.NewReader(file)
	shared, metadata := false, false
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "must-never-enter-the-archive-59827") {
			t.Fatal("system secret leaked into archive")
		}
		if strings.Contains(header.Name, "shared/config.js") {
			shared = true
		}
		if header.Name == "metadata.json" {
			var value map[string]any
			if err := json.Unmarshal(data, &value); err != nil {
				t.Fatal(err)
			}
			metadata = true
		}
	}
	if !shared || !metadata {
		t.Fatal("archive lost shared imports or metadata")
	}
	if _, err := createK6Archive("../../secret"); err == nil {
		t.Fatal("accepted script traversal")
	}
}
