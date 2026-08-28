package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckOperatorTestsWorkflowAcceptsPinnedDefaultSuite(t *testing.T) {
	t.Parallel()

	source := []byte(`jobs:
  Tests-Operator:
    steps:
      - run: >
          nix develop --command bash -c "unset GOROOT;
          cd misc/operator && go test ./..."
`)

	require.NoError(t, checkOperatorTestsWorkflow(source))
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
      - run: nix develop --command bash -c "unset GOROOT; cd misc/operator && go test -tags integration ./..."
`,
		"conditional job": `jobs:
  Tests-Operator:
    if: github.event_name == 'push'
    steps:
      - run: nix develop --command bash -c "unset GOROOT; cd misc/operator && go test ./..."
`,
		"allowed failure": `jobs:
  Tests-Operator:
    continue-on-error: true
    steps:
      - run: nix develop --command bash -c "unset GOROOT; cd misc/operator && go test ./..."
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
