package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckGoSourceDetectsImportedCallsWithoutTextFalsePositives(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

import clock "time"

func TestExample() {
	clock.Sleep(1)
	_ = "time.Sleep(1)"
	// time.Sleep(1)
}
`)

	findings, err := checkGoSource("sample_test.go", source)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Equal(t, 6, findings[0].line)
}

func TestCheckGoSourceDetectsDotImportedEnvironmentReads(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

import . "os"

func read() string {
	return Getenv("LOCAL_POLICY")
}
`)

	findings, err := checkGoSource("internal/infra/state/nested/sample.go", source)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Equal(t, 6, findings[0].line)
}

func TestCheckGoSourceIgnoresShadowedPackageNames(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

import "time"

type sleeper struct{}

func (s sleeper) Sleep(int) {}

func TestExample() {
	time := sleeper{}
	time.Sleep(1)
}

var _ = time.Second
`)

	findings, err := checkGoSource("sample_test.go", source)
	require.NoError(t, err)
	require.Empty(t, findings)
}

func TestCheckProtoSourceDetectsMultilineAndInlineDeclarations(t *testing.T) {
	t.Parallel()

	source := []byte(`syntax = "proto3";

message Example {
	reserved
		2,
		4;
	string value = 1; reserved "old_value";
}
`)

	findings := checkProtoSource("misc/proto/example.proto", source)
	require.Len(t, findings, 2)
	require.Equal(t, 4, findings[0].line)
	require.Equal(t, 7, findings[1].line)
}

func TestCheckProtoSourceIgnoresCommentsAndStrings(t *testing.T) {
	t.Parallel()

	source := []byte(`syntax = "proto3";

// reserved 1;
/*
reserved 2;
*/
message Example {
	string note = 1 [default = "reserved 3;"];
}
`)

	require.Empty(t, checkProtoSource("misc/proto/example.proto", source))
}

func TestIsDeterministicFSMPathIncludesNestedPackages(t *testing.T) {
	t.Parallel()

	require.True(t, isDeterministicFSMPath("internal/domain/processing/numscript/example.go"))
	require.True(t, isDeterministicFSMPath("internal/infra/plan/planerr/example.go"))
	require.False(t, isDeterministicFSMPath("internal/application/admission/example.go"))
}

func TestRequiredCIResultGateFailsClosed(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		needs       string
		wantSuccess bool
		wantOutput  string
	}{
		{
			name:        "all success",
			needs:       `{"Dirty":{"result":"success"},"Tests":{"result":"success"}}`,
			wantSuccess: true,
			wantOutput:  "Required CI: PASS",
		},
		{
			name:        "optional skipped outside needs",
			needs:       `{"Mandatory":{"result":"success"}}`,
			wantSuccess: true,
			wantOutput:  "Required CI: PASS",
		},
		{
			name:       "producer failure",
			needs:      `{"Dirty":{"result":"failure"},"Tests":{"result":"success"}}`,
			wantOutput: "REQUIRED_CI_PRODUCER_NOT_SUCCESS: Dirty=failure",
		},
		{
			name:       "producer cancelled",
			needs:      `{"Dirty":{"result":"success"},"Tests":{"result":"cancelled"}}`,
			wantOutput: "REQUIRED_CI_PRODUCER_NOT_SUCCESS: Tests=cancelled",
		},
		{
			name:       "producer skipped",
			needs:      `{"Dirty":{"result":"skipped"},"Tests":{"result":"success"}}`,
			wantOutput: "REQUIRED_CI_PRODUCER_NOT_SUCCESS: Dirty=skipped",
		},
		{
			name:       "unknown conclusion",
			needs:      `{"Dirty":{"result":"future-conclusion"}}`,
			wantOutput: "REQUIRED_CI_PRODUCER_NOT_SUCCESS: Dirty=future-conclusion",
		},
		{
			name:       "missing conclusion",
			needs:      `{"Dirty":{}}`,
			wantOutput: "REQUIRED_CI_PRODUCER_NOT_SUCCESS: Dirty=missing",
		},
		{
			name:       "empty needs",
			needs:      `{}`,
			wantOutput: "REQUIRED_CI_INVALID_INPUT",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			command := exec.Command("bash", "required-ci")
			command.Env = append(os.Environ(), "REQUIRED_CI_NEEDS="+testCase.needs)
			output, err := command.CombinedOutput()
			if testCase.wantSuccess {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
			}
			require.Contains(t, string(output), testCase.wantOutput)
		})
	}
}

func TestRequiredCITopologyAcceptsAggregatedAndExplicitlyOptionalJobs(t *testing.T) {
	t.Parallel()

	findings, err := checkRequiredCITopology(validRequiredCIWorkflows(), validRequiredCIContract())
	require.NoError(t, err)
	require.Empty(t, findings)
}

func TestRequiredCITopologyRejectsOmittedPRJob(t *testing.T) {
	t.Parallel()

	workflows := validRequiredCIWorkflows()
	workflows[requiredCIWorkflowPath] = []byte(strings.Replace(
		string(workflows[requiredCIWorkflowPath]),
		"  Optional:\n",
		"  Omitted:\n    runs-on: ubuntu-latest\n  Optional:\n",
		1,
	))

	findings, err := checkRequiredCITopology(workflows, validRequiredCIContract())
	require.NoError(t, err)
	require.Contains(t, findingMessages(findings),
		"UNAGGREGATED_PR_JOB: .github/workflows/main.yml#Omitted")
}

func TestRequiredCITopologyRejectsMandatoryJobRename(t *testing.T) {
	t.Parallel()

	workflows := validRequiredCIWorkflows()
	workflows[requiredCIWorkflowPath] = []byte(strings.Replace(
		string(workflows[requiredCIWorkflowPath]),
		"  Mandatory:\n",
		"  Renamed:\n",
		1,
	))

	findings, err := checkRequiredCITopology(workflows, validRequiredCIContract())
	require.NoError(t, err)
	messages := findingMessages(findings)
	require.Contains(t, messages, "UNKNOWN_REQUIRED_CI_NEED: Mandatory")
	require.Contains(t, messages,
		"UNAGGREGATED_PR_JOB: .github/workflows/main.yml#Renamed")
}

func TestRequiredCITopologyRejectsPRJobInAnotherWorkflow(t *testing.T) {
	t.Parallel()

	workflows := validRequiredCIWorkflows()
	workflows[".github/workflows/other.yml"] = []byte(`name: Other
on: [push, pull_request]
jobs:
  Detached:
    runs-on: ubuntu-latest
`)

	findings, err := checkRequiredCITopology(workflows, validRequiredCIContract())
	require.NoError(t, err)
	require.Contains(t, findingMessages(findings),
		"UNAGGREGATED_PR_JOB: .github/workflows/other.yml#Detached")
}

func TestRequiredCITopologyRejectsAggregatedOptionalJob(t *testing.T) {
	t.Parallel()

	workflows := validRequiredCIWorkflows()
	workflows[requiredCIWorkflowPath] = []byte(strings.Replace(
		string(workflows[requiredCIWorkflowPath]),
		"    needs: [Mandatory]\n",
		"    needs: [Mandatory, Optional]\n",
		1,
	))

	findings, err := checkRequiredCITopology(workflows, validRequiredCIContract())
	require.NoError(t, err)
	require.Contains(t, findingMessages(findings),
		"OPTIONAL_PR_JOB_AGGREGATED: .github/workflows/main.yml#Optional")
}

func TestRequiredCITopologyRejectsPullRequestFilters(t *testing.T) {
	t.Parallel()

	workflows := validRequiredCIWorkflows()
	workflows[requiredCIWorkflowPath] = []byte(strings.Replace(
		string(workflows[requiredCIWorkflowPath]),
		"    types: [opened, synchronize, reopened]\n",
		"    types: [opened, synchronize, reopened]\n    paths: [internal/**]\n",
		1,
	))

	findings, err := checkRequiredCITopology(workflows, validRequiredCIContract())
	require.NoError(t, err)
	require.Contains(t, findingMessages(findings),
		"REQUIRED_CI_PULL_REQUEST_FILTER_FORBIDDEN: paths")
}

func validRequiredCIWorkflows() map[string][]byte {
	return map[string][]byte{
		requiredCIWorkflowPath: []byte(`name: Default
on:
  pull_request:
    types: [opened, synchronize, reopened]
jobs:
  Mandatory:
    runs-on: ubuntu-latest
  Optional:
    if: false
    runs-on: ubuntu-latest
  Required-CI:
    name: Required CI
    if: always()
    needs: [Mandatory]
    runs-on: ubuntu-latest
    env:
      REQUIRED_CI_NEEDS: ${{ toJSON(needs) }}
    steps:
      - run: nix develop --command bash scripts/required-ci
`),
		".github/workflows/release.yml": []byte(`name: Release
on:
  push:
    tags: [v*]
jobs:
  Publish:
    runs-on: ubuntu-latest
`),
	}
}

func validRequiredCIContract() []byte {
	return []byte(`{
  "aggregateJob": "Required-CI",
  "optionalJobs": {
    ".github/workflows/main.yml#Optional": "opt-in publication"
  }
}`)
}

func findingMessages(findings []finding) []string {
	messages := make([]string, 0, len(findings))
	for _, item := range findings {
		messages = append(messages, item.message)
	}

	return messages
}
