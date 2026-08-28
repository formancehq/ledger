package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModelWorkloadTestReachabilityAcceptsDedicatedRaceJob(t *testing.T) {
	t.Parallel()

	findings, err := checkModelWorkloadTestReachability(defaultCIWorkflowPath, validModelWorkflow())
	require.NoError(t, err)
	require.Empty(t, findings)
}

func TestModelWorkloadTestReachabilityRejectsMissingRaceTest(t *testing.T) {
	t.Parallel()

	source := strings.ReplaceAll(
		string(validModelWorkflow()),
		modelUnitTestCommand,
		"nix develop --command go -C tests/antithesis/workload test ./...",
	)

	findings, err := checkModelWorkloadTestReachability(defaultCIWorkflowPath, []byte(source))
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].message, "MODEL_WORKLOAD_TESTS_UNREACHABLE")
}

func TestModelWorkloadTestReachabilityRejectsNonMandatoryRaceJob(t *testing.T) {
	t.Parallel()

	tests := map[string][]byte{
		"conditional job": []byte(`jobs:
  Tests-Antithesis-Workload:
    if: false
    steps:
      - run: nix develop --command go -C tests/antithesis/workload test -race ./...
`),
		"conditional step": []byte(`jobs:
  Tests-Antithesis-Workload:
    steps:
      - if: false
        run: nix develop --command go -C tests/antithesis/workload test -race ./...
`),
		"ignored failure": []byte(`jobs:
  Tests-Antithesis-Workload:
    steps:
      - continue-on-error: true
        run: nix develop --command go -C tests/antithesis/workload test -race ./...
`),
	}

	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			findings, err := checkModelWorkloadTestReachability(defaultCIWorkflowPath, source)
			require.NoError(t, err)
			require.NotEmpty(t, findings)
		})
	}
}

func TestModelCampaignReachabilityRejectsNonMandatoryCampaign(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"conditional job": strings.Replace(
			string(validModelWorkflow()),
			"  Tests-Model:\n",
			"  Tests-Model:\n    if: false\n",
			1,
		),
		"ignored failure": strings.Replace(
			string(validModelWorkflow()),
			"      - run: "+modelCampaignCommand+"\n",
			"      - continue-on-error: true\n        run: "+modelCampaignCommand+"\n",
			1,
		),
	}

	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			findings, err := checkModelWorkloadTestReachability(defaultCIWorkflowPath, []byte(source))
			require.NoError(t, err)
			require.NotEmpty(t, findings)
			require.Contains(t, findings[0].message, "MODEL_CAMPAIGN")
		})
	}
}

func TestModelWorkloadTestReachabilityRejectsTestInAnotherJob(t *testing.T) {
	t.Parallel()

	source := []byte(`jobs:
  Tests-Antithesis-Workload:
    steps:
      - run: echo no workload tests here
  Tests-Model:
    steps:
      - run: nix develop --command just test-model-cluster 180
  Other:
    steps:
      - run: nix develop --command go -C tests/antithesis/workload test -race ./...
`)

	findings, err := checkModelWorkloadTestReachability(defaultCIWorkflowPath, source)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].message, "MODEL_WORKLOAD_TESTS_UNREACHABLE")
}

func TestModelWorkloadTestReachabilityRejectsMissingRuntimeCampaign(t *testing.T) {
	t.Parallel()

	source := strings.ReplaceAll(
		string(validModelWorkflow()),
		modelCampaignCommand,
		"nix develop --command just test-model 180",
	)

	findings, err := checkModelWorkloadTestReachability(defaultCIWorkflowPath, []byte(source))
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].message, "MODEL_CAMPAIGN_UNREACHABLE")
}

func TestModelWorkloadTestReachabilityRejectsCombinedModelJob(t *testing.T) {
	t.Parallel()

	source := []byte(`jobs:
  Tests-Model:
    steps:
      - run: nix develop --command just test-model-cluster 180
      - run: nix develop --command go -C tests/antithesis/workload test -race ./...
`)

	findings, err := checkModelWorkloadTestReachability(defaultCIWorkflowPath, source)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].message, "missing Tests-Antithesis-Workload job")
}

func validModelWorkflow() []byte {
	return []byte(`jobs:
  Tests-Antithesis-Workload:
    steps:
      - run: nix develop --command go -C tests/antithesis/workload test -race ./...
  Tests-Model:
    steps:
      - run: nix develop --command just test-model-cluster 180
`)
}
