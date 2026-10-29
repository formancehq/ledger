package main

import (
	"archive/tar"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func testIdentity() runIdentity {
	return runIdentity{Commit: strings.Repeat("a", 40), Image: "ghcr.io/formancehq/ledger:pr-42-aaaaaaa@sha256:" + strings.Repeat("b", 64), RunID: "123456", Attempt: "1", PR: "42"}
}

func TestWorkflowImageVerification(t *testing.T) {
	t.Parallel()
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct {
				Name string
				Run  string
				Env  map[string]string
			}
		}
	}
	data, err := os.ReadFile("../../.github/workflows/perf.yml")
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	var script string
	for _, step := range workflow.Jobs["Submit"].Steps {
		if step.Name == "Resolve the image built by this run" {
			_, body, found := strings.Cut(step.Run, "<<'IMAGE'\n")
			if !found || !strings.HasSuffix(body, "IMAGE\n") {
				t.Fatal("image verification block not found")
			}
			script = strings.TrimSuffix(body, "IMAGE\n")
		}
		if step.Name == "Pulumi up with a local backend" {
			if step.Env["IMAGE"] != "${{ steps.image.outputs.image }}" || !strings.Contains(step.Run, "perf:image=$IMAGE") {
				t.Fatal("Pulumi must consume the verified digest-pinned output")
			}
		}
	}
	if script == "" {
		t.Fatal("missing workflow image verification")
	}
	sha := strings.Repeat("a", 40)
	digest := "sha256:" + strings.Repeat("b", 64)
	config := `{"config":{"Labels":{"org.opencontainers.image.revision":"` + sha + `","org.opencontainers.image.source":"https://github.com/formancehq/ledger"}}}`
	for _, tt := range []struct {
		name, digest, config string
		valid                bool
	}{
		{"valid", digest, config, true},
		{"all-platforms-valid", digest, `{"linux/amd64":` + config + `,"linux/arm64":` + config + `}`, true},
		{"wrong-commit", digest, strings.ReplaceAll(config, sha, strings.Repeat("c", 40)), false},
		{"wrong-source", digest, strings.ReplaceAll(config, "formancehq/ledger", "other/repo"), false},
		{"missing-platform-labels", digest, `{"linux/amd64":{},"linux/arm64":` + config + `}`, false},
		{"missing-labels", digest, `{"config":{}}`, false},
		{"bad-json", digest, "invalid-json", false},
		{"bad-digest", "sha256:bad", config, false},
		{"missing-digest", "", config, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			// The workflow's real Bash runs against a registry process fixture.
			// No persistent helper, registry access or Kubernetes call is needed.
			fake := "#!/bin/sh\ncase \"$*\" in\n  *'.Manifest.Digest'*) printf '%s\\n' \"$REGISTRY_DIGEST\" ;;\n  *'.Image'*) printf '%s\\n' \"$REGISTRY_CONFIG\" ;;\n  *) exit 2 ;;\nesac\n"
			if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(fake), 0o700); err != nil {
				t.Fatal(err)
			}
			output := filepath.Join(dir, "output")
			cmd := exec.CommandContext(t.Context(), "bash", "-euo", "pipefail", "-c", script)
			cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"),
				"SHA="+sha, "IMAGE_REPOSITORY=ghcr.io/formancehq/ledger", "IMAGE_TAG=pr-42-aaaaaaa-run-123-2",
				"GITHUB_REPOSITORY=formancehq/ledger", "GITHUB_OUTPUT="+output,
				"REGISTRY_DIGEST="+tt.digest, "REGISTRY_CONFIG="+tt.config)
			logs, err := cmd.CombinedOutput()
			if tt.valid && err != nil {
				t.Fatalf("valid image rejected: %v: %s", err, logs)
			}
			if !tt.valid && err == nil {
				t.Fatal("invalid image accepted")
			}
			result, readErr := os.ReadFile(output)
			if tt.valid {
				want := "image=ghcr.io/formancehq/ledger:pr-42-aaaaaaa-run-123-2@" + digest + "\n"
				if readErr != nil || string(result) != want {
					t.Fatalf("wrong submitted reference: %q (%v)", result, readErr)
				}
			} else if len(result) != 0 {
				t.Fatal("rejected image produced a submission reference")
			}
		})
	}
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
