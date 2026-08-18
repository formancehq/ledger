package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecideApprove(t *testing.T) {
	t.Parallel()

	action, blockers, err := decide(reviewResult{
		Decision: "APPROVE",
		Findings: []finding{{
			ID:          "note",
			Severity:    "P3",
			Blocking:    new(false),
			AutoFixable: new(false),
		}},
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
			{ID: "one", Severity: "P1", Blocking: new(true), AutoFixable: new(true)},
			{ID: "two", Severity: "P2", Blocking: new(true), AutoFixable: new(true)},
			{ID: "note", Severity: "P3", Blocking: new(false), AutoFixable: new(false)},
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
			{ID: "one", Severity: "P1", Blocking: new(true), AutoFixable: new(true)},
			{ID: "two", Severity: "P2", Blocking: new(true), AutoFixable: new(false)},
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
		Findings: []finding{{
			ID:          "one",
			Severity:    "P1",
			Blocking:    new(true),
			AutoFixable: new(false),
		}},
	})
	require.ErrorContains(t, err, "APPROVE contains blocking findings")

	_, _, err = decide(reviewResult{Decision: "REQUEST_CHANGES"})
	require.ErrorContains(t, err, "REQUEST_CHANGES has no blocking findings")
}

func TestDecideRejectsMissingBlockingFlags(t *testing.T) {
	t.Parallel()

	_, _, err := decide(reviewResult{
		Decision: "APPROVE",
		Findings: []finding{{ID: "one", Severity: "P1"}},
	})
	require.ErrorContains(t, err, "missing explicit blocking flags")
}

func TestDecideHonorsExplicitHumanDecision(t *testing.T) {
	t.Parallel()

	action, _, err := decide(reviewResult{Decision: "HUMAN_DECISION_REQUIRED"})
	require.NoError(t, err)
	require.Equal(t, actionHuman, action)
}

func TestLoadReviewResultValidatesSchema(t *testing.T) {
	t.Parallel()

	_, err := loadReviewResult(writeReviewResult(t, `{
		"decision":"APPROVE",
		"head":"abc",
		"worktree_fingerprint":"fingerprint",
		"residual_risk":"EXTREME",
		"findings":[]
	}`))
	require.ErrorContains(t, err, "invalid residual_risk")

	_, err = loadReviewResult(writeReviewResult(t, `{
		"decision":"REQUEST_CHANGES",
		"head":"abc",
		"worktree_fingerprint":"fingerprint",
		"residual_risk":"HIGH",
		"findings":[{
			"id":"x","severity":"CRITICAL","blocking":true,"auto_fixable":true,
			"title":"x","evidence":"x","impact":"x","resolution":"x"
		}]
	}`))
	require.ErrorContains(t, err, "invalid severity")

	_, err = loadReviewResult(writeReviewResult(t, `{
		"decision":"APPROVE",
		"head":"abc",
		"worktree_fingerprint":"fingerprint",
		"residual_risk":"LOW",
		"findings":[{
			"id":"x","severity":"P0","blokcing":true,"auto_fixable":false,
			"title":"x","evidence":"x","impact":"x","resolution":"x"
		}]
	}`))
	require.ErrorContains(t, err, "unknown field")

	_, err = loadReviewResult(writeReviewResult(t, `{
		"decision":"APPROVE",
		"head":"abc",
		"worktree_fingerprint":"fingerprint",
		"residual_risk":"LOW",
		"findings":[{
			"id":"x","severity":"P0","auto_fixable":false,
			"title":"x","evidence":"x","impact":"x","resolution":"x"
		}]
	}`))
	require.ErrorContains(t, err, "missing explicit blocking flags")
}

func TestValidateReviewTarget(t *testing.T) {
	t.Parallel()

	expected := workspaceState{Head: "abc", Fingerprint: "fingerprint"}
	require.NoError(t, validateReviewTarget(reviewResult{
		Head:                "abc",
		WorktreeFingerprint: "fingerprint",
	}, expected))

	err := validateReviewTarget(reviewResult{
		Head:                "stale",
		WorktreeFingerprint: "fingerprint",
	}, expected)
	require.ErrorContains(t, err, "reviewed head mismatch")

	err = validateReviewTarget(reviewResult{
		Head:                "abc",
		WorktreeFingerprint: "stale",
	}, expected)
	require.ErrorContains(t, err, "reviewed worktree fingerprint mismatch")
}

func TestCreateRunStateDirIsolatesRuns(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	first, err := createRunStateDir(parent)
	require.NoError(t, err)
	second, err := createRunStateDir(parent)
	require.NoError(t, err)

	require.NotEqual(t, first, second)
	require.DirExists(t, first)
	require.DirExists(t, second)
}

func TestCaptureWorkspaceStateTracksContent(t *testing.T) {
	t.Parallel()

	repository := t.TempDir()
	runGit(t, repository, "init")
	require.NoError(t, os.WriteFile(filepath.Join(repository, "tracked.txt"), []byte("initial\n"), 0o644))
	runGit(t, repository, "add", "tracked.txt")
	runGit(t, repository, "-c", "user.name=Review Loop Test", "-c", "user.email=review-loop@example.com", "commit", "-m", "initial")

	clean, err := captureWorkspaceState(repository)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(repository, "tracked.txt"), []byte("changed\n"), 0o644))
	trackedChange, err := captureWorkspaceState(repository)
	require.NoError(t, err)
	require.NotEqual(t, clean.Fingerprint, trackedChange.Fingerprint)

	require.NoError(t, os.WriteFile(filepath.Join(repository, "untracked.txt"), []byte("one\n"), 0o644))
	untrackedOne, err := captureWorkspaceState(repository)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(repository, "untracked.txt"), []byte("two\n"), 0o644))
	untrackedTwo, err := captureWorkspaceState(repository)
	require.NoError(t, err)
	require.NotEqual(t, untrackedOne.Fingerprint, untrackedTwo.Fingerprint)

	stateRoot := filepath.Join(repository, ".review-state")
	stateDirectory := filepath.Join(stateRoot, "run-test")
	require.NoError(t, os.MkdirAll(stateDirectory, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(stateDirectory, "review-1.json"), []byte("state\n"), 0o644))
	withState, err := captureWorkspaceState(repository)
	require.NoError(t, err)
	withoutState, err := captureWorkspaceState(repository, stateRoot)
	require.NoError(t, err)
	require.NotEqual(t, untrackedTwo.Fingerprint, withState.Fingerprint)
	require.Equal(t, untrackedTwo.Fingerprint, withoutState.Fingerprint)
}

func writeReviewResult(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "review.json")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	return path
}

func runGit(t *testing.T, directory string, arguments ...string) {
	t.Helper()

	cmd := exec.Command("git", arguments...)
	cmd.Dir = directory
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
}
