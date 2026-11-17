package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"
)

const (
	operatorTestsWorkflowPath  = ".github/workflows/main.yml"
	operatorTestsJobID         = "Tests-Operator"
	operatorTestsCommand       = `nix develop .#ci --command bash -c "unset GOROOT; cd misc/operator && go test ./..."`
	operatorIntegrationCommand = `nix develop .#operator-tests --command just operator-test-integration`
	operatorJustfilePath       = "justfile"
	operatorIntegrationRecipe  = `#!/usr/bin/env bash
set -euo pipefail
unset GOROOT
KUBEBUILDER_ASSETS=$(setup-envtest use 1.32.x -p path)
export KUBEBUILDER_ASSETS
cd misc/operator
go test -race -tags integration ./... -count=1 -parallel 4 -timeout 20m`
)

func main() {
	checks := []struct {
		path  string
		check func([]byte) error
	}{
		{operatorTestsWorkflowPath, checkOperatorTestsWorkflow},
		{operatorJustfilePath, checkOperatorIntegrationRecipe},
	}
	for _, check := range checks {
		source, err := os.ReadFile(check.path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "check-operator-tests-reachability: reading %s: %v\n", check.path, err)
			os.Exit(1)
		}
		if err := check.check(source); err != nil {
			fmt.Fprintf(os.Stderr, "%s:1:1: ERROR: %v\n", check.path, err)
			fmt.Fprintln(os.Stderr, "check-operator-tests-reachability: FAIL")
			os.Exit(1)
		}
	}

	fmt.Println("check-operator-tests-reachability: PASS")
}

func checkOperatorTestsWorkflow(source []byte) error {
	var workflow struct {
		Jobs map[string]struct {
			If              any `yaml:"if"`
			ContinueOnError any `yaml:"continue-on-error"`
			Steps           []struct {
				If              any    `yaml:"if"`
				ContinueOnError any    `yaml:"continue-on-error"`
				Run             string `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}

	if err := yaml.Unmarshal(source, &workflow); err != nil {
		return fmt.Errorf("parse workflow YAML: %w", err)
	}

	unitReachable, integrationReachable := false, false
	job, ok := workflow.Jobs[operatorTestsJobID]
	if ok && job.If == nil && job.ContinueOnError == nil {
		for _, step := range job.Steps {
			if step.If != nil || step.ContinueOnError != nil {
				continue
			}
			switch normalizeWorkflowCommand(step.Run) {
			case operatorTestsCommand:
				unitReachable = true
			case operatorIntegrationCommand:
				integrationReachable = true
			}
		}
	}

	if !unitReachable {
		return fmt.Errorf(
			"operator unit tests must remain reachable from the unconditional %q job through the pinned command %q",
			operatorTestsJobID,
			operatorTestsCommand,
		)
	}
	if !integrationReachable {
		return fmt.Errorf(
			"all operator integration tests must remain reachable from the unconditional %q job through the pinned command %q",
			operatorTestsJobID,
			operatorIntegrationCommand,
		)
	}

	return nil
}

func checkOperatorIntegrationRecipe(source []byte) error {
	var body []string
	active := false
	definitions := 0
	for line := range strings.Lines(string(source)) {
		line = strings.TrimSuffix(line, "\n")
		if line == "operator-test-integration:" {
			definitions++
			active = true

			continue
		}
		if active && line != "" && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") {
			active = false
		}
		if active {
			body = append(body, line)
		}
	}
	if definitions != 1 || strings.Join(nonemptyRecipeLines(strings.Join(body, "\n")), "\n") != strings.Join(nonemptyRecipeLines(operatorIntegrationRecipe), "\n") {
		return errors.New("operator-test-integration must run the complete unfiltered race suite with pinned Kubernetes 1.32.x envtest assets")
	}

	return nil
}

func nonemptyRecipeLines(source string) []string {
	var lines []string
	for line := range strings.Lines(source) {
		line = strings.TrimSpace(line)
		if line != "" {
			lines = append(lines, line)
		}
	}

	return lines
}

func normalizeWorkflowCommand(command string) string {
	return strings.Join(strings.Fields(command), " ")
}
