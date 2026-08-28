package main

import (
	"fmt"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"
)

const (
	operatorTestsWorkflowPath = ".github/workflows/main.yml"
	operatorTestsJobID        = "Tests-Operator"
	operatorTestsCommand      = `nix develop --command bash -c "unset GOROOT; cd misc/operator && go test ./..."`
)

func main() {
	source, err := os.ReadFile(operatorTestsWorkflowPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "check-operator-tests-reachability: reading %s: %v\n", operatorTestsWorkflowPath, err)
		os.Exit(1)
	}

	if err := checkOperatorTestsWorkflow(source); err != nil {
		fmt.Fprintf(os.Stderr, "%s:1:1: ERROR: %v\n", operatorTestsWorkflowPath, err)
		fmt.Fprintln(os.Stderr, "check-operator-tests-reachability: FAIL")
		os.Exit(1)
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

	job, ok := workflow.Jobs[operatorTestsJobID]
	if ok && job.If == nil && job.ContinueOnError == nil {
		for _, step := range job.Steps {
			if step.If == nil && step.ContinueOnError == nil && normalizeWorkflowCommand(step.Run) == operatorTestsCommand {
				return nil
			}
		}
	}

	return fmt.Errorf(
		"operator unit tests must remain reachable from the unconditional %q job through the pinned command %q",
		operatorTestsJobID,
		operatorTestsCommand,
	)
}

func normalizeWorkflowCommand(command string) string {
	return strings.Join(strings.Fields(command), " ")
}
