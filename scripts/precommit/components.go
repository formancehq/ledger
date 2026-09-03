package main

import (
	"path/filepath"
	"strings"
)

const componentMapVersion = "ledger-precommit-components-v1"

type component struct {
	Name       string
	Recipe     string
	Inputs     func(string) bool
	Outputs    func(string) bool
	Config     func(string) bool
	Complete   bool
	MappingWhy string
}

func componentMap() []component {
	return []component{
		{
			Name:       "fuzz-inventory",
			Recipe:     "fuzz-inventory-check",
			Inputs:     fuzzInventoryInput,
			Outputs:    noPath,
			Config:     fuzzInventoryConfig,
			Complete:   true,
			MappingWhy: "all test sources, module boundaries, runner inventory, and checker implementation are covered",
		},
		{
			Name:       "mock-code-generation",
			Recipe:     "generate",
			Inputs:     mockGenerationInput,
			Outputs:    mockGenerationOutput,
			Config:     commonGeneratorConfig,
			Complete:   true,
			MappingWhy: "all root-module Go sources that can add directives or affect generated interfaces are covered",
		},
		{
			Name:       "proto-generation",
			Recipe:     "generate-proto",
			Inputs:     protoGenerationInput,
			Outputs:    protoGenerationOutput,
			Config:     protoGenerationConfig,
			Complete:   true,
			MappingWhy: "proto sources, custom plugins, pinned protoc toolchain, recipe, and generated protobuf outputs are covered",
		},
		{
			Name:       "operator-generation",
			Recipe:     "operator-generate",
			Inputs:     operatorGenerationInput,
			Outputs:    operatorGenerationOutput,
			Config:     operatorGenerationConfig,
			Complete:   true,
			MappingWhy: "operator Go annotations, controller-gen identity, RBAC sync script, and every committed output are covered",
		},
		{
			Name:       "dashboards",
			Recipe:     "test-dashboards",
			Inputs:     dashboardInput,
			Outputs:    dashboardOutput,
			Config:     dashboardConfig,
			Complete:   true,
			MappingWhy: "Jsonnet, lockfile/vendor identity, dashboard tests, toolchain, and committed JSON outputs are covered",
		},
		{
			Name:       "tidy-root",
			Recipe:     "tidy-root",
			Inputs:     rootTidyInput,
			Outputs:    rootModuleOutput,
			Config:     goToolConfig,
			Complete:   true,
			MappingWhy: "every root-module Go source and root module manifest is covered",
		},
		{
			Name:       "tidy-operator",
			Recipe:     "tidy-operator",
			Inputs:     operatorTidyInput,
			Outputs:    operatorModuleOutput,
			Config:     goToolConfig,
			Complete:   true,
			MappingWhy: "every operator-module Go source and module manifest is covered",
		},
		{
			Name:       "tidy-model-workload",
			Recipe:     "tidy-model-workload",
			Inputs:     workloadTidyInput,
			Outputs:    workloadModuleOutput,
			Config:     goToolConfig,
			Complete:   true,
			MappingWhy: "every workload-module Go source and module manifest is covered",
		},
		{
			Name:       "lint-root",
			Recipe:     "lint-root",
			Inputs:     rootLintInput,
			Outputs:    rootModuleGo,
			Config:     lintConfig,
			Complete:   true,
			MappingWhy: "all root-module Go, module manifests, lint configuration, and pinned linter identity are covered",
		},
		{
			Name:       "lint-operator",
			Recipe:     "lint-operator",
			Inputs:     operatorLintInput,
			Outputs:    operatorGo,
			Config:     lintConfig,
			Complete:   true,
			MappingWhy: "all operator-module Go, module manifests, lint configuration, and pinned linter identity are covered",
		},
	}
}

func normalizePath(path string) string {
	return filepath.ToSlash(filepath.Clean(path))
}

func noPath(string) bool { return false }

func hasPrefix(path, prefix string) bool {
	return path == strings.TrimSuffix(prefix, "/") || strings.HasPrefix(path, prefix)
}

func isGo(path string) bool { return strings.HasSuffix(path, ".go") }

func isModuleManifest(path, directory string) bool {
	prefix := ""
	if directory != "" {
		prefix = strings.TrimSuffix(directory, "/") + "/"
	}

	return path == prefix+"go.mod" || path == prefix+"go.sum"
}

func nestedModule(path string) bool {
	for _, prefix := range []string{
		"misc/devenv/monitoring-dashboards/",
		"misc/operator/",
		"tests/antithesis/workload/",
		"tests/perf/",
		"tools/protoc-gen-dethash/",
		"tools/protoc-gen-queryfilter-validity/",
		"tools/protoc-gen-reader/",
		"tools/protoc-gen-skippable/",
	} {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}

	return false
}

func rootModuleGo(path string) bool { return isGo(path) && !nestedModule(path) }
func operatorGo(path string) bool   { return strings.HasPrefix(path, "misc/operator/") && isGo(path) }

func fuzzInventoryInput(path string) bool {
	return strings.HasSuffix(path, "_test.go") || strings.HasSuffix(path, "/go.mod") || path == "go.mod" ||
		path == "scripts/fuzz-targets.txt"
}

func fuzzInventoryConfig(path string) bool {
	return path == "scripts/check-repo-invariants" || path == "scripts/check-repo-invariants.go" ||
		path == "scripts/fuzz_inventory.go"
}

func mockGenerationOutput(path string) bool {
	if !strings.HasPrefix(path, "internal/") || !isGo(path) {
		return false
	}
	name := filepath.Base(path)

	return strings.Contains(name, "_generated")
}

func mockGenerationInput(path string) bool {
	return (rootModuleGo(path) && !mockGenerationOutput(path)) || isModuleManifest(path, "") || protoGenerationOutput(path)
}

func commonGeneratorConfig(path string) bool {
	return path == "justfile" || path == "flake.nix" || path == "flake.lock"
}

func protoGenerationInput(path string) bool {
	if strings.HasPrefix(path, "misc/proto/") && strings.HasSuffix(path, ".proto") {
		return true
	}
	for _, prefix := range []string{
		"tools/protoc-gen-dethash/",
		"tools/protoc-gen-queryfilter-validity/",
		"tools/protoc-gen-reader/",
		"tools/protoc-gen-skippable/",
	} {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}

	return false
}

func protoGenerationOutput(path string) bool {
	return strings.HasPrefix(path, "internal/proto/") && strings.HasSuffix(path, ".pb.go")
}

func protoGenerationConfig(path string) bool { return commonGeneratorConfig(path) }

func operatorGenerationOutput(path string) bool {
	return path == "misc/operator/api/v1alpha1/zz_generated.deepcopy.go" ||
		hasPrefix(path, "misc/operator/config/crd/bases/") && strings.HasSuffix(path, ".yaml") ||
		hasPrefix(path, "misc/operator/config/rbac/") && strings.HasSuffix(path, ".yaml") ||
		hasPrefix(path, "misc/operator/helm/crds/templates/") && strings.HasSuffix(path, ".yaml") ||
		path == "misc/operator/helm/operator/templates/clusterrole.yaml"
}

func operatorGenerationInput(path string) bool {
	return operatorGo(path) && !operatorGenerationOutput(path) ||
		isModuleManifest(path, "misc/operator") || path == "misc/operator/scripts/sync-chart-rbac.sh"
}

func operatorGenerationConfig(path string) bool { return commonGeneratorConfig(path) }

func dashboardOutput(path string) bool {
	return strings.HasPrefix(path, "misc/devenv/monitoring-dashboards/config/dashboards/") &&
		strings.HasSuffix(path, ".json")
}

func dashboardInput(path string) bool {
	return strings.HasPrefix(path, "misc/devenv/monitoring-dashboards/jsonnet/") ||
		strings.HasPrefix(path, "misc/devenv/monitoring-dashboards/") && isGo(path) ||
		isModuleManifest(path, "misc/devenv/monitoring-dashboards")
}

func dashboardConfig(path string) bool { return commonGeneratorConfig(path) }

func rootTidyInput(path string) bool    { return rootModuleGo(path) || isModuleManifest(path, "") }
func rootModuleOutput(path string) bool { return isModuleManifest(path, "") }

func operatorTidyInput(path string) bool {
	return operatorGo(path) || isModuleManifest(path, "misc/operator")
}
func operatorModuleOutput(path string) bool { return isModuleManifest(path, "misc/operator") }

func workloadTidyInput(path string) bool {
	return strings.HasPrefix(path, "tests/antithesis/workload/") && isGo(path) ||
		isModuleManifest(path, "tests/antithesis/workload")
}

func workloadModuleOutput(path string) bool {
	return isModuleManifest(path, "tests/antithesis/workload")
}

func rootLintInput(path string) bool {
	return rootModuleGo(path) || isModuleManifest(path, "") || path == ".golangci.yaml"
}
func operatorLintInput(path string) bool {
	return operatorGo(path) || isModuleManifest(path, "misc/operator") || path == ".golangci.yaml"
}

func goToolConfig(path string) bool {
	return path == "flake.nix" || path == "flake.lock" || path == "justfile"
}

func lintConfig(path string) bool { return goToolConfig(path) || path == ".golangci.yaml" }

func affectsEveryComponent(path string) bool {
	return path == "justfile" || path == "flake.nix" || path == "flake.lock" ||
		hasPrefix(path, "scripts/precommit/")
}

func knownIrrelevant(path string) bool {
	return hasPrefix(path, "docs/") || strings.HasSuffix(path, ".md") ||
		hasPrefix(path, ".github/") || hasPrefix(path, "scripts/")
}
