package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckOperatorTestsWorkflowAcceptsPinnedDefaultSuite(t *testing.T) {
	t.Parallel()

	source := []byte(`jobs:
  Tests-Operator:
    steps:
      - run: >
          nix develop .#ci --command bash -c "unset GOROOT;
          cd misc/operator && go test ./..."
      - run: >
          nix develop .#operator-tests --command just
          operator-test-integration
`)

	require.NoError(t, checkOperatorTestsWorkflow(source))
}

func TestCheckOperatorTestsWorkflowRejectsWeakenedIntegrationRunner(t *testing.T) {
	t.Parallel()

	const source = `jobs:
  Tests-Operator:
    steps:
      - run: nix develop .#ci --command bash -c "unset GOROOT; cd misc/operator && go test ./..."
      - run: nix develop .#operator-tests --command just operator-test-integration
`
	tests := map[string]string{
		"missing integration step": strings.ReplaceAll(source, "      - run: "+operatorIntegrationCommand+"\n", ""),
		"admission subset only":    strings.ReplaceAll(source, "operator-test-integration", "operator-test-admission"),
		"default shell":            strings.ReplaceAll(source, ".#operator-tests", "."),
		"CI shell":                 strings.ReplaceAll(source, ".#operator-tests", ".#ci"),
		"conditional step": strings.ReplaceAll(source, "      - run: "+operatorIntegrationCommand,
			"      - if: github.event_name == 'push'\n        run: "+operatorIntegrationCommand),
		"allowed step failure": strings.ReplaceAll(source, "      - run: "+operatorIntegrationCommand,
			"      - continue-on-error: true\n        run: "+operatorIntegrationCommand),
		"ignored command failure": strings.ReplaceAll(source, operatorIntegrationCommand, operatorIntegrationCommand+" || true"),
		"separate conditional job": strings.ReplaceAll(source, "      - run: "+operatorIntegrationCommand,
			"  Integration:\n    if: github.event_name == 'push'\n    steps:\n      - run: "+operatorIntegrationCommand),
	}
	for name, candidate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.ErrorContains(t, checkOperatorTestsWorkflow([]byte(candidate)), "all operator integration tests must remain reachable")
		})
	}
}

func TestCheckOperatorIntegrationRecipeAcceptsCompleteSuite(t *testing.T) {
	t.Parallel()

	source := "operator-test-integration:\n    " + strings.ReplaceAll(operatorIntegrationRecipe, "\n", "\n    ") + "\n\nother-recipe:\n    go build ./...\n"
	require.NoError(t, checkOperatorIntegrationRecipe([]byte(source)))
}

func TestCheckOperatorIntegrationRecipeRejectsWeakenedSuite(t *testing.T) {
	t.Parallel()

	const source = "operator-test-integration:\n    " + "#!/usr/bin/env bash\n" +
		"    set -euo pipefail\n" +
		"    unset GOROOT\n" +
		"    KUBEBUILDER_ASSETS=$(setup-envtest use 1.32.x -p path)\n" +
		"    export KUBEBUILDER_ASSETS\n" +
		"    cd misc/operator\n" +
		"    go test -race -tags integration ./... -count=1 -parallel 4 -timeout 20m\n"
	tests := map[string]string{
		"missing recipe":             "other-recipe:\n    go test ./...\n",
		"collapsed shebang and body": "operator-test-integration:\n    " + normalizeWorkflowCommand(operatorIntegrationRecipe) + "\n",
		"joined shell commands":      strings.ReplaceAll(source, "set -euo pipefail\n    unset GOROOT", "set -euo pipefail unset GOROOT"),
		"joined comment and test":    strings.ReplaceAll(source, "    go test", "    # entire test body go test"),
		"duplicate recipe":           source + source,
		"test filter":                strings.ReplaceAll(source, "./...", "./... -run '^TestLedgerAdmission'"),
		"short mode":                 strings.ReplaceAll(source, "go test", "go test -short"),
		"missing integration tag":    strings.ReplaceAll(source, "-tags integration ", ""),
		"missing race detector":      strings.ReplaceAll(source, "-race ", ""),
		"one package only":           strings.ReplaceAll(source, "./...", "./internal/controller"),
		"ignored test failure":       strings.ReplaceAll(source, "-timeout 20m", "-timeout 20m || true"),
		"conditional test command":   strings.ReplaceAll(source, "go test", "false && go test"),
		"missing strict shell":       strings.ReplaceAll(source, "set -euo pipefail", "set -uo pipefail"),
		"unpinned assets":            strings.ReplaceAll(source, "use 1.32.x -p path", "use -p path"),
		"unexported assets":          strings.ReplaceAll(source, "    export KUBEBUILDER_ASSETS\n", ""),
		"wrong module":               strings.ReplaceAll(source, "cd misc/operator", "cd misc"),
	}
	for name, candidate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.ErrorContains(t, checkOperatorIntegrationRecipe([]byte(candidate)), "complete unfiltered race suite")
		})
	}
}

func TestCheckOperatorTestsRepositoryConfiguration(t *testing.T) {
	t.Parallel()

	workflow, err := os.ReadFile(filepath.Join("..", "..", operatorTestsWorkflowPath))
	require.NoError(t, err)
	require.NoError(t, checkOperatorTestsWorkflow(workflow))
	justfile, err := os.ReadFile(filepath.Join("..", "..", operatorJustfilePath))
	require.NoError(t, err)
	require.NoError(t, checkOperatorIntegrationRecipe(justfile))
}

func TestCheckOperatorTestsWorkflowRejectsMissingOrWeakenedRunner(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"missing job": `jobs:
  Build:
    steps:
      - run: go build ./...
`,
		"outside pinned Nix": `jobs:
  Tests-Operator:
    steps:
      - run: cd misc/operator && go test ./...
`,
		"integration tags": `jobs:
  Tests-Operator:
    steps:
      - run: nix develop .#ci --command bash -c "unset GOROOT; cd misc/operator && go test -tags integration ./..."
`,
		"conditional job": `jobs:
  Tests-Operator:
    if: github.event_name == 'push'
    steps:
      - run: nix develop .#ci --command bash -c "unset GOROOT; cd misc/operator && go test ./..."
`,
		"default dev shell": `jobs:
  Tests-Operator:
    steps:
      - run: nix develop --command bash -c "unset GOROOT; cd misc/operator && go test ./..."
`,
		"allowed failure": `jobs:
  Tests-Operator:
    continue-on-error: true
    steps:
      - run: nix develop .#ci --command bash -c "unset GOROOT; cd misc/operator && go test ./..."
`,
	}

	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := checkOperatorTestsWorkflow([]byte(source))
			require.ErrorContains(t, err, "operator unit tests must remain reachable")
		})
	}
}
