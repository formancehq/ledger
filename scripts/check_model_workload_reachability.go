package main

import (
	"fmt"
	"strings"

	"go.yaml.in/yaml/v3"
)

const (
	defaultCIWorkflowPath = ".github/workflows/main.yml"
	modelWorkloadTestJob  = "Tests-Antithesis-Workload"
	modelCampaignJob      = "Tests-Model"
	modelUnitTestCommand  = "nix develop --command go -C tests/antithesis/workload test -race ./..."
	modelCampaignCommand  = "nix develop --command just test-model-cluster 180"
)

type workflowReachability struct {
	Jobs map[string]workflowReachabilityJob `yaml:"jobs"`
}

type workflowReachabilityJob struct {
	If    any                        `yaml:"if"`
	Steps []workflowReachabilityStep `yaml:"steps"`
}

type workflowReachabilityStep struct {
	Run             string `yaml:"run"`
	If              any    `yaml:"if"`
	ContinueOnError any    `yaml:"continue-on-error"`
}

func checkModelWorkloadTestReachability(path string, source []byte) ([]finding, error) {
	var workflow workflowReachability
	if err := yaml.Unmarshal(source, &workflow); err != nil {
		return nil, fmt.Errorf("parsing workflow YAML: %w", err)
	}

	job, ok := workflow.Jobs[modelWorkloadTestJob]
	if !ok {
		return []finding{workflowFinding(
			path,
			fmt.Sprintf("MODEL_WORKLOAD_TESTS_UNREACHABLE: missing %s job", modelWorkloadTestJob),
		)}, nil
	}
	if workflowConditionSet(job.If) {
		return []finding{workflowFinding(
			path,
			fmt.Sprintf("MODEL_WORKLOAD_TESTS_CONDITIONAL: %s must not have a job-level if condition", modelWorkloadTestJob),
		)}, nil
	}

	testStep := -1
	for index, step := range job.Steps {
		command := strings.Join(strings.Fields(step.Run), " ")
		if command == modelUnitTestCommand {
			if !workflowConditionSet(step.If) &&
				!workflowFailureIgnored(step.ContinueOnError) &&
				testStep < 0 {
				testStep = index
			}
		}
	}

	if testStep < 0 {
		return []finding{workflowFinding(
			path,
			fmt.Sprintf(
				"MODEL_WORKLOAD_TESTS_UNREACHABLE: %s must run %q",
				modelWorkloadTestJob,
				modelUnitTestCommand,
			),
		)}, nil
	}

	campaign, ok := workflow.Jobs[modelCampaignJob]
	if !ok {
		return []finding{workflowFinding(
			path,
			fmt.Sprintf("MODEL_CAMPAIGN_UNREACHABLE: missing %s job", modelCampaignJob),
		)}, nil
	}
	if workflowConditionSet(campaign.If) {
		return []finding{workflowFinding(
			path,
			fmt.Sprintf("MODEL_CAMPAIGN_CONDITIONAL: %s must not have a job-level if condition", modelCampaignJob),
		)}, nil
	}

	campaignStep := -1
	for index, step := range campaign.Steps {
		command := strings.Join(strings.Fields(step.Run), " ")
		if command == modelCampaignCommand &&
			!workflowConditionSet(step.If) &&
			!workflowFailureIgnored(step.ContinueOnError) &&
			campaignStep < 0 {
			campaignStep = index
		}
	}
	if campaignStep < 0 {
		return []finding{workflowFinding(
			path,
			fmt.Sprintf(
				"MODEL_CAMPAIGN_UNREACHABLE: %s must run %q",
				modelCampaignJob,
				modelCampaignCommand,
			),
		)}, nil
	}

	return nil, nil
}

func workflowFailureIgnored(value any) bool {
	if value == nil {
		return false
	}

	ignored, ok := value.(bool)

	return !ok || ignored
}

func workflowConditionSet(value any) bool {
	if value == nil {
		return false
	}

	if condition, ok := value.(string); ok {
		return strings.TrimSpace(condition) != ""
	}

	return true
}

func workflowFinding(path, message string) finding {
	return finding{
		path:    path,
		line:    1,
		column:  1,
		message: message,
	}
}
