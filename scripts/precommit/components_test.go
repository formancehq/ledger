package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInputSensitiveSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		path         string
		content      string
		wantSelected []string
		wantReason   string
		wantFull     bool
	}{
		{
			name:         "docs only skips expensive components",
			path:         "docs/guide.md",
			wantSelected: nil,
		},
		{
			name:         "proto source selects proto generation",
			path:         "misc/proto/common.proto",
			wantSelected: []string{"proto-generation"},
			wantReason:   "input_changed:misc/proto/common.proto",
		},
		{
			name:         "manual generated proto output edit fails closed",
			path:         "internal/proto/commonpb/common.pb.go",
			wantSelected: []string{"mock-code-generation", "proto-generation", "tidy-root", "lint-root"},
			wantReason:   "declared_output_changed:internal/proto/commonpb/common.pb.go",
		},
		{
			name:         "operator API selects operator generator",
			path:         "misc/operator/api/v1alpha1/cluster_types.go",
			wantSelected: []string{"operator-generation", "tidy-operator", "lint-operator"},
			wantReason:   "input_changed:misc/operator/api/v1alpha1/cluster_types.go",
		},
		{
			name:         "dashboard Jsonnet selects dashboard generation",
			path:         "misc/devenv/monitoring-dashboards/jsonnet/main.jsonnet",
			wantSelected: []string{"dashboards"},
			wantReason:   "input_changed:misc/devenv/monitoring-dashboards/jsonnet/main.jsonnet",
		},
		{
			name:         "root Go import selects tidy and lint",
			path:         "internal/example.go",
			wantSelected: []string{"tidy-root", "lint-root"},
		},
		{
			name:         "new generator directive selects generation",
			path:         "scripts/fresh_generator.go",
			content:      "package main\n//go:generate mockgen -destination generated.go example Interface\n",
			wantSelected: []string{"mock-code-generation", "tidy-root", "lint-root"},
			wantReason:   "input_changed:scripts/fresh_generator.go",
		},
		{
			name:         "nested workload change selects its tidy",
			path:         "tests/antithesis/workload/example.go",
			wantSelected: []string{"tidy-model-workload"},
		},
		{
			name:         "unknown path forces full fallback",
			path:         "mystery/input.txt",
			wantSelected: componentNames(componentMap()),
			wantFull:     true,
		},
		{
			name:         "lint configuration selects both lint components",
			path:         ".golangci.yaml",
			wantSelected: []string{"lint-root", "lint-operator"},
		},
		{
			name:         "candidate selection map change forces trusted full fallback",
			path:         "scripts/precommit/components.go",
			wantSelected: componentNames(componentMap()),
			wantFull:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newSelectionFixture(t)
			if test.content == "" {
				fixture.change(t, test.path)
			} else {
				writeFixtureFile(t, fixture.candidate, test.path, test.content)
			}
			plan := fixture.plan(t)
			selected := selectedNames(plan, componentMap())
			require.Equal(t, test.wantSelected, selected)
			require.Equal(t, test.wantFull, plan.FullFallback)
			if test.wantReason != "" {
				var found bool
				for _, reasons := range plan.Selected {
					found = found || slices.Contains(reasons, test.wantReason)
				}
				require.True(t, found, "selection must retain the exact trusted input/output edge")
			}
		})
	}
}

func TestToolIdentityMismatchRunsEveryComponent(t *testing.T) {
	t.Parallel()

	fixture := newSelectionFixture(t)
	fixture.change(t, "docs/guide.md")
	r := runner{repo: fixture.candidate, toolRoot: fixture.trusted, components: componentMap()}
	plan, err := r.selectComponents(fixture.base, strings.Repeat("0", 40))
	require.NoError(t, err)
	require.True(t, plan.FullFallback)
	require.Contains(t, plan.FallbackReasons, "trusted_tool_sha_mismatch")
	require.Equal(t, componentNames(componentMap()), selectedNames(plan, componentMap()))
}

func TestUnversionedDashboardVendorCannotBeSkipped(t *testing.T) {
	t.Parallel()

	fixture := newSelectionFixture(t)
	fixture.change(t, "docs/guide.md")
	vendorFile := filepath.Join(fixture.candidate, "misc", "devenv", "monitoring-dashboards", "jsonnet", "vendor", "library.libsonnet")
	require.NoError(t, os.MkdirAll(filepath.Dir(vendorFile), 0o755))
	require.NoError(t, os.WriteFile(vendorFile, []byte("vendor\n"), 0o644))
	plan := fixture.plan(t)
	require.Contains(t, selectedNames(plan, componentMap()), "dashboards")
	require.Contains(t, plan.Selected["dashboards"], "untrusted_or_unversioned_jsonnet_vendor")
}

// These exact reason assertions are the sensitivity proof for the most
// important edges. Removing the proto input edge changes the selection to an
// unknown-path fallback; removing the generated-output edge omits the proto
// generator entirely because other Go mappings still recognize the path.
func TestSensitivityEdgesRemainExplicit(t *testing.T) {
	t.Parallel()

	fixture := newSelectionFixture(t)
	fixture.change(t, "misc/proto/common.proto")
	protoPlan := fixture.plan(t)
	require.False(t, protoPlan.FullFallback)
	require.Equal(t, []string{"input_changed:misc/proto/common.proto"}, protoPlan.Selected["proto-generation"])

	outputFixture := newSelectionFixture(t)
	outputFixture.change(t, "internal/proto/commonpb/common.pb.go")
	outputPlan := outputFixture.plan(t)
	require.False(t, outputPlan.FullFallback)
	require.Contains(t, outputPlan.Selected["proto-generation"],
		"declared_output_changed:internal/proto/commonpb/common.pb.go")
}

type selectionFixture struct {
	base      string
	trusted   string
	candidate string
}

func newSelectionFixture(t *testing.T) selectionFixture {
	t.Helper()

	root := t.TempDir()
	seed := filepath.Join(root, "seed")
	trusted := filepath.Join(root, "trusted")
	candidate := filepath.Join(root, "candidate")
	require.NoError(t, os.MkdirAll(seed, 0o755))
	runGit(t, seed, "init", "-q")
	runGit(t, seed, "config", "user.name", "Precommit Test")
	runGit(t, seed, "config", "user.email", "precommit@example.com")
	files := map[string]string{
		"docs/guide.md":                                                      "guide\n",
		"misc/proto/common.proto":                                            "syntax = \"proto3\";\n",
		"internal/proto/commonpb/common.pb.go":                               "// Code generated. DO NOT EDIT.\npackage commonpb\n",
		"misc/operator/api/v1alpha1/cluster_types.go":                        "package v1alpha1\n",
		"misc/devenv/monitoring-dashboards/jsonnet/main.jsonnet":             "{}\n",
		"internal/example.go":                                                "package internal\n",
		"tests/antithesis/workload/example.go":                               "package workload\n",
		".golangci.yaml":                                                     "version: '2'\n",
		"mystery/input.txt":                                                  "known at base but relationship unknown\n",
		"scripts/precommit/components.go":                                    "trusted map\n",
		"misc/devenv/monitoring-dashboards/jsonnet/.gitignore":               "vendor/\n",
		"misc/devenv/monitoring-dashboards/config/dashboards/generated.json": "{}\n",
		"misc/operator/go.mod":                                               "module example/operator\n",
		"tests/antithesis/workload/go.mod":                                   "module example/workload\n",
		"go.mod":                                                             "module example/root\n",
	}
	for path, content := range files {
		writeFixtureFile(t, seed, path, content)
	}
	runGit(t, seed, "add", ".")
	runGit(t, seed, "commit", "-qm", "base")
	base := runGitOutput(t, seed, "rev-parse", "HEAD")
	runGit(t, root, "clone", "-q", seed, trusted)
	runGit(t, root, "clone", "-q", seed, candidate)

	return selectionFixture{base: base, trusted: trusted, candidate: candidate}
}

func (fixture selectionFixture) change(t *testing.T, path string) {
	t.Helper()
	fullPath := filepath.Join(fixture.candidate, filepath.FromSlash(path))
	require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0o755))
	file, err := os.OpenFile(fullPath, os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = file.WriteString("changed\n")
	require.NoError(t, err)
	require.NoError(t, file.Close())
}

func (fixture selectionFixture) plan(t *testing.T) selection {
	t.Helper()
	r := runner{repo: fixture.candidate, toolRoot: fixture.trusted, components: componentMap()}
	plan, err := r.selectComponents(fixture.base, fixture.base)
	require.NoError(t, err)

	return plan
}

func componentNames(components []component) []string {
	names := make([]string, 0, len(components))
	for _, item := range components {
		names = append(names, item.Name)
	}

	return names
}

func selectedNames(plan selection, components []component) []string {
	var selected []string
	for _, item := range components {
		if _, ok := plan.Selected[item.Name]; ok {
			selected = append(selected, item.Name)
		}
	}

	return selected
}

func writeFixtureFile(t *testing.T, root, path, content string) {
	t.Helper()
	fullPath := filepath.Join(root, filepath.FromSlash(path))
	require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0o755))
	require.NoError(t, os.WriteFile(fullPath, []byte(content), 0o644))
}

func runGit(t *testing.T, directory string, arguments ...string) {
	t.Helper()
	_ = runGitOutput(t, directory, arguments...)
}

func runGitOutput(t *testing.T, directory string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, fmt.Sprintf("git %s:\n%s", strings.Join(arguments, " "), output))

	return strings.TrimSpace(string(output))
}
