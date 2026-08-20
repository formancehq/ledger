package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
		"human_decision_context":"",
		"previous_findings":[],
		"findings":[]
	}`))
	require.ErrorContains(t, err, "invalid residual_risk")

	_, err = loadReviewResult(writeReviewResult(t, `{
		"decision":"REQUEST_CHANGES",
		"head":"abc",
		"worktree_fingerprint":"fingerprint",
		"residual_risk":"HIGH",
		"human_decision_context":"",
		"previous_findings":[],
		"findings":[{
			"id":"x","severity":"CRITICAL","blocking":true,"auto_fixable":true,
			"title":"x","location":"","evidence":"x","impact":"x","resolution":"x"
		}]
	}`))
	require.ErrorContains(t, err, "invalid severity")

	_, err = loadReviewResult(writeReviewResult(t, `{
		"decision":"APPROVE",
		"head":"abc",
		"worktree_fingerprint":"fingerprint",
		"residual_risk":"LOW",
		"human_decision_context":"",
		"previous_findings":[],
		"findings":[{
			"id":"x","severity":"P0","blokcing":true,"auto_fixable":false,
			"title":"x","location":"","evidence":"x","impact":"x","resolution":"x"
		}]
	}`))
	require.ErrorContains(t, err, "unknown field")

	_, err = loadReviewResult(writeReviewResult(t, `{
		"decision":"APPROVE",
		"head":"abc",
		"worktree_fingerprint":"fingerprint",
		"residual_risk":"LOW",
		"human_decision_context":"",
		"previous_findings":[],
		"findings":[{
			"id":"x","severity":"P0","auto_fixable":false,
			"title":"x","location":"","evidence":"x","impact":"x","resolution":"x"
		}]
	}`))
	require.ErrorContains(t, err, "missing explicit blocking flags")
}

func TestLoadReviewResultRequiresCompleteStructuredContract(t *testing.T) {
	t.Parallel()

	_, err := loadReviewResult(writeReviewResult(t, `{
		"decision":"APPROVE",
		"head":"abc",
		"worktree_fingerprint":"fingerprint",
		"residual_risk":"LOW",
		"human_decision_context":"",
		"findings":[]
	}`))
	require.ErrorContains(t, err, "previous_findings")

	_, err = loadReviewResult(writeReviewResult(t, `{
		"decision":"APPROVE",
		"head":"abc",
		"worktree_fingerprint":"fingerprint",
		"residual_risk":"LOW",
		"previous_findings":[],
		"findings":[]
	}`))
	require.ErrorContains(t, err, "human_decision_context")

	_, err = loadReviewResult(writeReviewResult(t, `{
		"decision":"APPROVE",
		"head":"abc",
		"worktree_fingerprint":"fingerprint",
		"residual_risk":"LOW",
		"human_decision_context":"",
		"previous_findings":[],
		"findings":[{
			"id":"x","severity":"P2","blocking":false,"auto_fixable":false,
			"title":"x","evidence":"x","impact":"x","resolution":"x"
		}]
	}`))
	require.ErrorContains(t, err, "location")
}

func TestValidatePreviousFindings(t *testing.T) {
	t.Parallel()

	previous := reviewResult{Findings: []finding{{ID: "fixed"}, {ID: "valid"}, {ID: "old"}}}
	result := reviewResult{
		PreviousFindings: []previousFinding{
			{ID: "fixed", Status: "FIXED", Reason: "the test now covers the failure"},
			{ID: "valid", Status: "STILL_VALID", Reason: "the path remains unchanged"},
			{ID: "old", Status: "OUTDATED", Reason: "the affected code was removed"},
		},
		Findings: []finding{{ID: "valid"}, {ID: "new"}},
	}
	require.NoError(t, validatePreviousFindings(result, &previous))

	err := validatePreviousFindings(reviewResult{
		PreviousFindings: []previousFinding{{ID: "valid", Status: "FIXED", Reason: "claimed fixed"}},
		Findings:         []finding{{ID: "valid"}},
	}, &reviewResult{Findings: []finding{{ID: "valid"}}})
	require.ErrorContains(t, err, "is FIXED but is still present")

	err = validatePreviousFindings(reviewResult{
		PreviousFindings: []previousFinding{{ID: "valid", Status: "STILL_VALID", Reason: "still applies"}},
	}, &reviewResult{Findings: []finding{{ID: "valid"}}})
	require.ErrorContains(t, err, "STILL_VALID but is absent")

	err = validatePreviousFindings(reviewResult{
		PreviousFindings: []previousFinding{{ID: "injected", Status: "OUTDATED", Reason: "not real"}},
	}, &reviewResult{Findings: []finding{{ID: "valid"}}})
	require.ErrorContains(t, err, "unknown id")

	err = validatePreviousFindings(reviewResult{PreviousFindings: []previousFinding{{ID: "unexpected", Status: "FIXED", Reason: "x"}}}, nil)
	require.ErrorContains(t, err, "first review")
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

	runGit(t, repository, "add", "tracked.txt")
	stagedChange, err := captureWorkspaceState(repository)
	require.NoError(t, err)
	require.NotEqual(t, trackedChange.Fingerprint, stagedChange.Fingerprint)
	require.NoError(t, os.WriteFile(filepath.Join(repository, "tracked.txt"), []byte("initial\n"), 0o644))
	stagedWithWorktreeReversal, err := captureWorkspaceState(repository)
	require.NoError(t, err)
	require.NotEqual(t, clean.Fingerprint, stagedWithWorktreeReversal.Fingerprint)
	require.NotEqual(t, stagedChange.Fingerprint, stagedWithWorktreeReversal.Fingerprint)

	require.NoError(t, os.WriteFile(filepath.Join(repository, "untracked.txt"), []byte("one\n"), 0o644))
	untrackedOne, err := captureWorkspaceState(repository)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(repository, "untracked.txt"), []byte("two\n"), 0o644))
	untrackedTwo, err := captureWorkspaceState(repository)
	require.NoError(t, err)
	require.NotEqual(t, untrackedOne.Fingerprint, untrackedTwo.Fingerprint)

	stateDirectory := filepath.Join(repository, "run-test")
	require.NoError(t, os.MkdirAll(stateDirectory, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(stateDirectory, "review-1.json"), []byte("state\n"), 0o644))
	withState, err := captureWorkspaceState(repository)
	require.NoError(t, err)
	withoutState, err := captureWorkspaceState(repository, stateDirectory)
	require.NoError(t, err)
	require.NotEqual(t, untrackedTwo.Fingerprint, withState.Fingerprint)
	require.Equal(t, untrackedTwo.Fingerprint, withoutState.Fingerprint)
}

func TestCaptureReviewChangeTargetUsesExplicitBaseForCleanMultiCommitBranch(t *testing.T) {
	t.Parallel()

	repository := t.TempDir()
	runGit(t, repository, "init")
	require.NoError(t, os.WriteFile(filepath.Join(repository, "base.txt"), []byte("base\n"), 0o644))
	runGit(t, repository, "add", "base.txt")
	runGit(t, repository, "-c", "user.name=Review Loop Test", "-c", "user.email=review-loop@example.com", "commit", "-m", "base")
	runGit(t, repository, "branch", "review-base")
	baseHead := strings.TrimSpace(runGitOutput(t, repository, "rev-parse", "HEAD"))

	require.NoError(t, os.WriteFile(filepath.Join(repository, "first.txt"), []byte("first\n"), 0o644))
	runGit(t, repository, "add", "first.txt")
	runGit(t, repository, "-c", "user.name=Review Loop Test", "-c", "user.email=review-loop@example.com", "commit", "-m", "first")
	require.NoError(t, os.WriteFile(filepath.Join(repository, "second.txt"), []byte("second\n"), 0o644))
	runGit(t, repository, "add", "second.txt")
	runGit(t, repository, "-c", "user.name=Review Loop Test", "-c", "user.email=review-loop@example.com", "commit", "-m", "second")

	state, err := captureWorkspaceState(repository)
	require.NoError(t, err)
	base, err := resolveReviewBase(repository, "review-base")
	require.NoError(t, err)
	target, err := captureReviewChangeTarget(repository, base, state)
	require.NoError(t, err)

	require.Equal(t, changeTargetKind, target.Kind)
	require.Equal(t, "review-base", target.BaseRef)
	require.Equal(t, baseHead, target.BaseSHA)
	require.Equal(t, baseHead, target.MergeBaseSHA)
	require.Equal(t, state.Head, target.Head)
	require.Equal(t, worktreeChangeKinds{Staged: true, Unstaged: true, Untracked: true}, target.WorktreeScope)
	require.Equal(t, worktreeChangeKinds{}, target.WorktreePresent)

	changedFiles := strings.Fields(runGitOutput(t, repository, "diff", "--name-only", target.MergeBaseSHA, target.Head, "--"))
	require.ElementsMatch(t, []string{"first.txt", "second.txt"}, changedFiles)
}

func TestCaptureReviewChangeTargetIncludesDirtyWorktreeCategories(t *testing.T) {
	t.Parallel()

	repository := t.TempDir()
	runGit(t, repository, "init")
	for _, name := range []string{"staged.txt", "unstaged.txt"} {
		require.NoError(t, os.WriteFile(filepath.Join(repository, name), []byte("base\n"), 0o644))
	}
	runGit(t, repository, "add", "staged.txt", "unstaged.txt")
	runGit(t, repository, "-c", "user.name=Review Loop Test", "-c", "user.email=review-loop@example.com", "commit", "-m", "base")
	runGit(t, repository, "branch", "review-base")

	require.NoError(t, os.WriteFile(filepath.Join(repository, "staged.txt"), []byte("staged\n"), 0o644))
	runGit(t, repository, "add", "staged.txt")
	require.NoError(t, os.WriteFile(filepath.Join(repository, "unstaged.txt"), []byte("unstaged\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repository, "untracked.txt"), []byte("untracked\n"), 0o644))

	state, err := captureWorkspaceState(repository)
	require.NoError(t, err)
	base, err := resolveReviewBase(repository, "review-base")
	require.NoError(t, err)
	target, err := captureReviewChangeTarget(repository, base, state)
	require.NoError(t, err)
	require.Equal(t, worktreeChangeKinds{Staged: true, Unstaged: true, Untracked: true}, target.WorktreePresent)

	targetPath := filepath.Join(t.TempDir(), "target.json")
	expected, err := writeReviewChangeTarget(targetPath, target)
	require.NoError(t, err)
	require.NoError(t, verifyFileUnchanged(targetPath, expected))

	var decoded reviewChangeTarget
	require.NoError(t, json.Unmarshal(expected, &decoded))
	require.Equal(t, target, decoded)

	require.NoError(t, os.WriteFile(targetPath, []byte("{}\n"), 0o644))
	require.ErrorContains(t, verifyFileUnchanged(targetPath, expected), "content changed")
}

func TestCaptureReviewChangeTargetExcludesRunStateDirectory(t *testing.T) {
	t.Parallel()

	repository := t.TempDir()
	runGit(t, repository, "init")
	require.NoError(t, os.WriteFile(filepath.Join(repository, "base.txt"), []byte("base\n"), 0o644))
	runGit(t, repository, "add", "base.txt")
	runGit(t, repository, "-c", "user.name=Review Loop Test", "-c", "user.email=review-loop@example.com", "commit", "-m", "base")
	runGit(t, repository, "branch", "review-base")

	runStateDirectory := filepath.Join(repository, "review-state", "run-test")
	require.NoError(t, os.MkdirAll(runStateDirectory, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(runStateDirectory, "review-1.json"), []byte("state\n"), 0o644))

	state, err := captureWorkspaceState(repository, runStateDirectory)
	require.NoError(t, err)
	base, err := resolveReviewBase(repository, "review-base")
	require.NoError(t, err)
	target, err := captureReviewChangeTarget(repository, base, state, runStateDirectory)
	require.NoError(t, err)
	require.False(t, target.WorktreePresent.Untracked)
	require.Empty(t, target.UntrackedPaths)

	require.NoError(t, os.WriteFile(filepath.Join(repository, "untracked.txt"), []byte("review me\n"), 0o644))
	target, err = captureReviewChangeTarget(repository, base, state, runStateDirectory)
	require.NoError(t, err)
	require.True(t, target.WorktreePresent.Untracked)
	require.Equal(t, []string{"untracked.txt"}, target.UntrackedPaths)
}

func TestVerifyFileSnapshotsUnchangedRejectsFixerStateMutation(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	findingsPath := filepath.Join(directory, "fix-1.json")
	resultPath := filepath.Join(directory, "review-1.json")
	require.NoError(t, os.WriteFile(findingsPath, []byte("findings\n"), 0o644))
	require.NoError(t, os.WriteFile(resultPath, []byte("review\n"), 0o644))

	snapshots, err := captureFileSnapshots(findingsPath, resultPath)
	require.NoError(t, err)
	require.NoError(t, verifyFileSnapshotsUnchanged(snapshots))

	require.NoError(t, os.WriteFile(resultPath, []byte("tampered\n"), 0o644))
	err = verifyFileSnapshotsUnchanged(snapshots)
	require.ErrorContains(t, err, resultPath+" content changed")
}

func TestApprovedReviewFailsWhenLocalValidationFails(t *testing.T) {
	t.Parallel()

	repository := t.TempDir()
	runGit(t, repository, "init")
	require.NoError(t, os.WriteFile(filepath.Join(repository, "base.txt"), []byte("base\n"), 0o644))
	runGit(t, repository, "add", "base.txt")
	runGit(t, repository, "-c", "user.name=Review Loop Test", "-c", "user.email=review-loop@example.com", "commit", "-m", "base")
	baseSHA := strings.TrimSpace(runGitOutput(t, repository, "rev-parse", "HEAD"))

	reviewer := filepath.Join(repository, "reviewer.sh")
	require.NoError(t, os.WriteFile(reviewer, []byte(`#!/usr/bin/env bash
set -euo pipefail
cat > "$AI_REVIEW_RESULT" <<EOF
{"decision":"APPROVE","head":"$AI_REVIEW_HEAD","worktree_fingerprint":"$AI_REVIEW_WORKTREE_FINGERPRINT","previous_findings":[],"findings":[],"residual_risk":"LOW","human_decision_context":""}
EOF
`), 0o755))
	validationBase := filepath.Join(t.TempDir(), "validation-base")
	validator := filepath.Join(repository, "validator.sh")
	require.NoError(t, os.WriteFile(validator, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s' "$AI_REVIEW_BASE_SHA" > "$TEST_VALIDATION_BASE"
exit 42
`), 0o755))

	binary := filepath.Join(t.TempDir(), "review-loop")
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Dir = "."
	output, err := build.CombinedOutput()
	require.NoError(t, err, string(output))

	command := exec.Command(binary,
		"--base", baseSHA,
		"--review-cmd", "bash reviewer.sh",
		"--validation-cmd", "bash validator.sh",
		"--state-dir", filepath.Join(repository, ".review-state"),
	)
	command.Dir = repository
	command.Env = append(os.Environ(), "TEST_VALIDATION_BASE="+validationBase)
	output, err = command.CombinedOutput()
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "local validation before readiness")
	require.Contains(t, string(output), "local validation failed before readiness")
	content, readErr := os.ReadFile(validationBase)
	require.NoError(t, readErr)
	require.Equal(t, baseSHA, string(content))

	require.NoError(t, os.WriteFile(validator, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf 'changed by validation\n' > base.txt
`), 0o755))
	command = exec.Command(binary,
		"--base", baseSHA,
		"--review-cmd", "bash reviewer.sh",
		"--validation-cmd", "bash validator.sh",
		"--state-dir", filepath.Join(repository, ".review-state"),
	)
	command.Dir = repository
	output, err = command.CombinedOutput()
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "local validation changed the approved workspace")
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

func runGitOutput(t *testing.T, directory string, arguments ...string) string {
	t.Helper()

	cmd := exec.Command("git", arguments...)
	cmd.Dir = directory
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	return string(output)
}
