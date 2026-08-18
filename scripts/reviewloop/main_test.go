package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecideApprove(t *testing.T) {
	t.Parallel()

	action, blockers, err := decide(reviewResult{
		Decision: "APPROVE",
		Findings: []finding{{ID: "note", Severity: "P3", Blocking: false}},
	})
	require.NoError(t, err)
	require.Equal(t, actionReady, action)
	require.Empty(t, blockers)
}

func TestDecideAutoFixesOnlyWhenEveryBlockerIsAutoFixable(t *testing.T) {
	t.Parallel()

	action, blockers, err := decide(reviewResult{
		Decision: "REQUEST_CHANGES",
		Findings: []finding{
			{ID: "one", Severity: "P1", Blocking: true, AutoFixable: true},
			{ID: "two", Severity: "P2", Blocking: true, AutoFixable: true},
			{ID: "note", Severity: "P3", Blocking: false, AutoFixable: false},
		},
	})
	require.NoError(t, err)
	require.Equal(t, actionAutoFix, action)
	require.Len(t, blockers, 2)
}

func TestDecideEscalatesNonAutoFixableBlocker(t *testing.T) {
	t.Parallel()

	action, blockers, err := decide(reviewResult{
		Decision: "REQUEST_CHANGES",
		Findings: []finding{
			{ID: "one", Severity: "P1", Blocking: true, AutoFixable: true},
			{ID: "two", Severity: "P2", Blocking: true, AutoFixable: false},
		},
	})
	require.NoError(t, err)
	require.Equal(t, actionHuman, action)
	require.Len(t, blockers, 2)
}

func TestDecideRejectsInconsistentResults(t *testing.T) {
	t.Parallel()

	_, _, err := decide(reviewResult{
		Decision: "APPROVE",
		Findings: []finding{{ID: "one", Severity: "P1", Blocking: true}},
	})
	require.ErrorContains(t, err, "APPROVE contains blocking findings")

	_, _, err = decide(reviewResult{Decision: "REQUEST_CHANGES"})
	require.ErrorContains(t, err, "REQUEST_CHANGES has no blocking findings")
}

func TestDecideHonorsExplicitHumanDecision(t *testing.T) {
	t.Parallel()

	action, _, err := decide(reviewResult{Decision: "HUMAN_DECISION_REQUIRED"})
	require.NoError(t, err)
	require.Equal(t, actionHuman, action)
}
