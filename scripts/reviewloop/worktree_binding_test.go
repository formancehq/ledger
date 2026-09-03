package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/scripts/internal/testenv"
)

type worktreeBindingFixture struct {
	root          string
	candidate     string
	validationDir string
	bindingFile   string
	head          string
	binary        string
	reviewer      string
	guard         string
}

const reviewLoopFixtureTimeout = 45 * time.Second

func TestReviewOnlyFlowBindsAgentToDedicatedCandidate(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)
	capture := filepath.Join(filepath.Dir(fixture.candidate), "agent-cwd")

	output, err := fixture.run(t, 123, fixture.candidate, fixture.bindingFile, map[string]string{
		"TEST_AGENT_CWD": capture,
	})
	require.NoError(t, err, output)
	require.Contains(t, output, "WORKTREE_BINDING_GATE=PASS role=review")
	require.Contains(t, output, "ROOT_UNCHANGED=PASS")
	require.Equal(t, canonicalTestPath(t, fixture.candidate), strings.TrimSpace(readTestFile(t, capture)))
	require.NotEqual(t, fixture.root, strings.TrimSpace(readTestFile(t, capture)))
}

func TestBindingRejectsRootAsCandidate(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)
	binding := filepath.Join(filepath.Dir(fixture.candidate), "root-binding.json")
	writeBindingFile(t, binding, 123, fixture.root, fixture.head, fixture.root)

	output, err := fixture.run(t, 123, fixture.root, binding, nil)
	require.Error(t, err, output)
	require.Contains(t, output, "ROOT_CHECKOUT_AS_CANDIDATE_FORBIDDEN")
	require.NotContains(t, output, "WORKTREE_BINDING_GATE=PASS")
}

func TestBindingRejectsWorktreeOwnedByAnotherPR(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)
	binding := filepath.Join(filepath.Dir(fixture.candidate), "other-pr-binding.json")
	writeBindingFile(t, binding, 456, fixture.candidate, fixture.head, fixture.root)

	output, err := fixture.run(t, 123, fixture.candidate, binding, nil)
	require.Error(t, err, output)
	require.Contains(t, output, "CROSS_PR_WORKTREE_CONTAMINATION")
	require.NotContains(t, output, "WORKTREE_BINDING_GATE=PASS")
}

func TestBindingRejectsStateDirectoryInRootBeforeCreatingIt(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)
	forbiddenStateDir := filepath.Join(fixture.root, "agent-review-state")
	command := fixture.command(123, fixture.candidate, fixture.validationDir, fixture.bindingFile, nil)
	for index, argument := range command.Args {
		if argument == "--state-dir" {
			command.Args[index+1] = forbiddenStateDir

			break
		}
	}

	output, err := command.CombinedOutput()
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "ROOT_CHECKOUT_STATE_DIR_FORBIDDEN")
	require.NoDirExists(t, forbiddenStateDir)
}

func TestBindingRejectsStateDirectorySymlinkedIntoRootBeforeCreatingIt(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)
	stateLink := filepath.Join(filepath.Dir(fixture.candidate), "root-state-link")
	require.NoError(t, os.Symlink(fixture.root, stateLink))
	forbiddenStateDir := filepath.Join(stateLink, "agent-review-state")
	command := fixture.command(123, fixture.candidate, fixture.validationDir, fixture.bindingFile, nil)
	for index, argument := range command.Args {
		if argument == "--state-dir" {
			command.Args[index+1] = forbiddenStateDir

			break
		}
	}

	output, err := command.CombinedOutput()
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "ROOT_CHECKOUT_STATE_DIR_FORBIDDEN")
	require.NoDirExists(t, filepath.Join(fixture.root, "agent-review-state"))
}

func TestRootBranchMutationIsDetected(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)
	branchBefore := strings.TrimSpace(runGitOutput(t, fixture.root, "rev-parse", "--abbrev-ref", "HEAD"))

	output, err := fixture.run(t, 123, fixture.candidate, fixture.bindingFile, map[string]string{
		"TEST_ROOT_MUTATION": "branch",
	})
	require.Error(t, err, output)
	require.Contains(t, output, "ROOT_MUTATION_DETECTED")
	require.NotEqual(t, branchBefore, strings.TrimSpace(runGitOutput(t, fixture.root, "rev-parse", "--abbrev-ref", "HEAD")))
}

func TestRootUntrackedMutationIsDetected(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)

	output, err := fixture.run(t, 123, fixture.candidate, fixture.bindingFile, map[string]string{
		"TEST_ROOT_MUTATION": "untracked",
	})
	require.Error(t, err, output)
	require.Contains(t, output, "ROOT_MUTATION_DETECTED")
	require.FileExists(t, filepath.Join(fixture.root, "agent-root-mutation.txt"))
}

func TestDirtyRootContentMutationIsDetectedWhenPorcelainIsUnchanged(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)
	require.NoError(t, os.WriteFile(filepath.Join(fixture.root, "base.txt"), []byte("dirty before agent\n"), 0o644))
	statusBefore := runGitOutput(t, fixture.root, "status", "--porcelain=v1", "--untracked-files=all")

	output, err := fixture.run(t, 123, fixture.candidate, fixture.bindingFile, map[string]string{
		"TEST_ROOT_MUTATION": "dirty-content",
	})
	require.Error(t, err, output)
	require.Contains(t, output, "ROOT_MUTATION_DETECTED")
	require.Contains(t, output, "root workspace content changed")
	require.Equal(t, statusBefore, runGitOutput(t, fixture.root, "status", "--porcelain=v1", "--untracked-files=all"))
	require.Equal(t, "different dirty content\n", readTestFile(t, filepath.Join(fixture.root, "base.txt")))
}

func TestIgnoredRootContentMutationIsDetected(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)
	require.NoError(t, os.WriteFile(filepath.Join(fixture.root, ".git", "info", "exclude"), []byte("ignored-secret.txt\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(fixture.root, "ignored-secret.txt"), []byte("secret before agent\n"), 0o600))
	require.Empty(t, runGitOutput(t, fixture.root, "status", "--porcelain=v1", "--untracked-files=all"))

	output, err := fixture.run(t, 123, fixture.candidate, fixture.bindingFile, map[string]string{
		"TEST_ROOT_MUTATION": "ignored-content",
	})
	require.Error(t, err, output)
	require.Contains(t, output, "ROOT_MUTATION_DETECTED")
	require.Contains(t, output, "root workspace content changed")
	require.Empty(t, runGitOutput(t, fixture.root, "status", "--porcelain=v1", "--untracked-files=all"))
	require.Equal(t, "different ignored content\n", readTestFile(t, filepath.Join(fixture.root, "ignored-secret.txt")))
}

func TestIgnoredNestedRepositoryDirectoryIsSupported(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)
	require.NoError(t, os.WriteFile(filepath.Join(fixture.root, ".git", "info", "exclude"), []byte("ignored-dir/\n"), 0o644))
	nestedRepository := filepath.Join(fixture.root, "ignored-dir")
	require.NoError(t, os.MkdirAll(nestedRepository, 0o755))
	runGit(t, nestedRepository, "init")
	require.Empty(t, runGitOutput(t, fixture.root, "status", "--porcelain=v1", "--untracked-files=all"))

	output, err := fixture.run(t, 123, fixture.candidate, fixture.bindingFile, nil)
	require.NoError(t, err, output)
	require.Contains(t, output, "ROOT_UNCHANGED=PASS")
}

func TestGitMutationGuardRefusesRootCheckout(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)
	branchBefore := strings.TrimSpace(runGitOutput(t, fixture.root, "rev-parse", "--abbrev-ref", "HEAD"))

	output, err := fixture.run(t, 123, fixture.candidate, fixture.bindingFile, map[string]string{
		"TEST_ROOT_MUTATION": "guarded-branch",
	})
	require.Error(t, err, output)
	require.Contains(t, output, "ROOT_CHECKOUT_GIT_MUTATION_FORBIDDEN")
	require.Contains(t, output, "ROOT_UNCHANGED=PASS")
	require.Equal(t, branchBefore, strings.TrimSpace(runGitOutput(t, fixture.root, "rev-parse", "--abbrev-ref", "HEAD")))
}

func TestGitMutationGuardDefaultsToDenyForRootSubcommands(t *testing.T) {
	for _, subcommand := range []string{"stash", "apply", "revert", "am"} {
		t.Run(subcommand, func(t *testing.T) {
			fixture := newWorktreeBindingFixture(t)
			output, err := fixture.run(t, 123, fixture.candidate, fixture.bindingFile, map[string]string{
				"TEST_ROOT_MUTATION": "guard-" + subcommand,
			})
			require.Error(t, err, output)
			require.Contains(t, output, "ROOT_CHECKOUT_GIT_MUTATION_FORBIDDEN command=git "+subcommand)
			require.Contains(t, output, "ROOT_UNCHANGED=PASS")
		})
	}
}

func TestGitMutationGuardRefusesOutputWritingOptionsInRoot(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)
	contentBefore := readTestFile(t, filepath.Join(fixture.root, "base.txt"))

	output, err := fixture.run(t, 123, fixture.candidate, fixture.bindingFile, map[string]string{
		"TEST_ROOT_MUTATION": "guard-output",
	})
	require.Error(t, err, output)
	require.Contains(t, output, "ROOT_CHECKOUT_GIT_MUTATION_FORBIDDEN command=git diff")
	require.Contains(t, output, "ROOT_UNCHANGED=PASS")
	require.Equal(t, contentBefore, readTestFile(t, filepath.Join(fixture.root, "base.txt")))
}

func TestGitMutationGuardRefusesUnregisteredChildWorktree(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)

	output, err := fixture.run(t, 123, fixture.candidate, fixture.bindingFile, map[string]string{
		"TEST_ROOT_MUTATION": "child-worktree",
	})
	require.Error(t, err, output)
	require.Contains(t, output, "UNREGISTERED_CHILD_WORKTREE_FORBIDDEN")
	require.NoDirExists(t, filepath.Join(filepath.Dir(fixture.candidate), "agent-child"))
}

func TestGitMutationGuardAllowsFixtureWorktreesInIndependentRepository(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)

	output, err := fixture.run(t, 123, fixture.candidate, fixture.bindingFile, map[string]string{
		"TEST_ROOT_MUTATION": "fixture-worktree",
	})
	require.NoError(t, err, output)
	require.Contains(t, output, "ROOT_UNCHANGED=PASS")
}

func TestGuardDoesNotExposeUnguardedGitEnvironment(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)

	output, err := fixture.run(t, 123, fixture.candidate, fixture.bindingFile, map[string]string{
		"TEST_ROOT_MUTATION": "guard-environment",
	})
	require.NoError(t, err, output)
	require.Contains(t, output, "ROOT_UNCHANGED=PASS")
}

func TestDirectGitBypassStillTriggersRootMutationDetection(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)
	unguardedGit := testenv.Command(t, "git").Path
	execPathOutput, execPathErr := exec.Command(unguardedGit, "--exec-path").Output()
	if execPathErr == nil {
		gitBesideExecPath := filepath.Join(filepath.Dir(filepath.Dir(strings.TrimSpace(string(execPathOutput)))), "bin", "git")
		if _, statErr := os.Stat(gitBesideExecPath); statErr == nil {
			unguardedGit = gitBesideExecPath
		}
	}

	output, err := fixture.run(t, 123, fixture.candidate, fixture.bindingFile, map[string]string{
		"TEST_ROOT_MUTATION": "direct-git-bypass",
		"TEST_UNGUARDED_GIT": unguardedGit,
	})
	require.Error(t, err, output)
	require.Contains(t, output, "ROOT_MUTATION_DETECTED")
	require.Contains(t, output, "root branch changed")
}

func TestGitMutationGuardAllowsReadOnlyRootInspection(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)

	output, err := fixture.run(t, 123, fixture.candidate, fixture.bindingFile, map[string]string{
		"TEST_ROOT_MUTATION": "guard-read",
	})
	require.NoError(t, err, output)
	require.Contains(t, output, "ROOT_UNCHANGED=PASS")
}

func TestGitMutationGuardRunsWithoutTempfilesUnderSystemBash(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)
	guardCopy := filepath.Join(fixture.validationDir, "system-bash-guard")
	guardContent, err := os.ReadFile(fixture.guard)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(guardCopy, guardContent, 0o700))
	realGit := testenv.Command(t, "git").Path
	readOnlyTemp := filepath.Join(fixture.validationDir, "read-only-tmp")
	require.NoError(t, os.Mkdir(readOnlyTemp, 0o500))

	command := exec.Command("/bin/bash", guardCopy, "status", "--short")
	command.Dir = fixture.candidate
	command.Env = testenv.EnvironmentMap(map[string]string{
		"PATH":                  filepath.Dir(guardCopy) + string(os.PathListSeparator) + os.Getenv("PATH"),
		"TMPDIR":                readOnlyTemp,
		"TRUSTED_ROOT_CHECKOUT": fixture.root,
	})
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output), realGit)
}

func TestConcurrentPRRunsUseDistinctWorktreesAndValidationDirs(t *testing.T) {
	outerGuardDirectory := filepath.Join(t.TempDir(), "git-guard-bin")
	require.NoError(t, os.Mkdir(outerGuardDirectory, 0o755))
	outerGuardMarker := filepath.Join(t.TempDir(), "outer-guard-used")
	require.NoError(t, os.WriteFile(filepath.Join(outerGuardDirectory, "git"), fmt.Appendf(nil,
		"#!/bin/sh\nprintf 'outer guard used\\n' >> %s\nexit 99\n",
		shellQuote(outerGuardMarker),
	), 0o755))
	t.Setenv("PATH", outerGuardDirectory+string(os.PathListSeparator)+os.Getenv("PATH"))
	for name, value := range map[string]string{
		"EXPECTED_PR_NUMBER":         "999999",
		"EXPECTED_WORKTREE":          "/inherited/outer/candidate",
		"EXPECTED_HEAD":              strings.Repeat("f", 40),
		"EXPECTED_FUTURE_BINDING":    "must-not-leak",
		"AI_WORKTREE_PR":             "999999",
		"AI_WORKTREE_PATH":           "/inherited/outer/candidate",
		"AI_WORKTREE_EXPECTED_HEAD":  strings.Repeat("e", 40),
		"AI_WORKTREE_BINDING_FILE":   "/inherited/outer/binding.json",
		"AI_WORKTREE_FUTURE_BINDING": "must-not-leak",
		"TRUSTED_ROOT_CHECKOUT":      "/inherited/outer/root",
		"AI_GIT_REAL_PATH":           "/inherited/outer/git",
		"AI_GIT_ORIGINAL_PATH":       "/inherited/outer/original-git",
		"AI_REVIEW_HEAD":             strings.Repeat("d", 40),
		"VALIDATION_RUN_DIR":         "/inherited/outer/validation",
		"VALIDATION_RUN_ID":          "inherited-outer-run",
	} {
		t.Setenv(name, value)
	}

	fixture := newWorktreeBindingFixture(t)
	parent := filepath.Dir(fixture.candidate)
	secondCandidate := filepath.Join(parent, "candidate-456")
	runGit(t, fixture.root, "worktree", "add", "--detach", secondCandidate, fixture.head)
	secondValidation := filepath.Join(parent, "validation-456")
	require.NoError(t, os.MkdirAll(secondValidation, 0o755))
	secondBinding := filepath.Join(parent, "binding-456.json")
	writeBindingFile(t, secondBinding, 456, secondCandidate, fixture.head, fixture.root)

	firstCWD := filepath.Join(parent, "cwd-123")
	secondCWD := filepath.Join(parent, "cwd-456")
	first := fixture.command(123, fixture.candidate, fixture.validationDir, fixture.bindingFile, map[string]string{
		"TEST_AGENT_CWD": firstCWD,
		"TEST_SYNC_FDS":  "1",
	})
	second := fixture.command(456, secondCandidate, secondValidation, secondBinding, map[string]string{
		"TEST_AGENT_CWD": secondCWD,
		"TEST_SYNC_FDS":  "1",
	})
	result, err := testenv.RunSynchronized(t, reviewLoopFixtureTimeout,
		testenv.SynchronizedCommand{Name: "PR 123", Command: first},
		testenv.SynchronizedCommand{Name: "PR 456", Command: second},
	)
	require.NoError(t, err)

	require.NotEqual(t, fixture.candidate, secondCandidate)
	require.NotEqual(t, fixture.validationDir, secondValidation)
	require.NotEqual(t, fixture.candidate, fixture.validationDir)
	require.NotEqual(t, secondCandidate, secondValidation)
	require.Equal(t, canonicalTestPath(t, fixture.candidate), strings.TrimSpace(readTestFile(t, firstCWD)))
	require.Equal(t, canonicalTestPath(t, secondCandidate), strings.TrimSpace(readTestFile(t, secondCWD)))
	require.Contains(t, result.Output["PR 123"], "WORKTREE_BINDING_GATE=PASS role=validation")
	require.Contains(t, result.Output["PR 456"], "WORKTREE_BINDING_GATE=PASS role=validation")
	require.NoFileExists(t, outerGuardMarker, "the enclosing AI run's Git guard must never execute")
}

func TestConcurrentPRRunAbortsSiblingWhenPeerFailsBeforeReady(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)
	parent := filepath.Dir(fixture.candidate)
	secondCandidate := filepath.Join(parent, "candidate-456")
	runGit(t, fixture.root, "worktree", "add", "--detach", secondCandidate, fixture.head)
	secondValidation := filepath.Join(parent, "validation-456")
	require.NoError(t, os.MkdirAll(secondValidation, 0o755))
	secondBinding := filepath.Join(parent, "binding-456.json")
	writeBindingFile(t, secondBinding, 456, secondCandidate, fixture.head, fixture.root)
	firstPID := filepath.Join(parent, "reviewer-123.pid")
	secondPID := filepath.Join(parent, "reviewer-456.pid")

	first := fixture.command(123, fixture.candidate, fixture.validationDir, fixture.bindingFile, map[string]string{
		"TEST_REVIEWER_PID": firstPID,
		"TEST_SYNC_FDS":     "1",
	})
	second := fixture.command(456, secondCandidate, secondValidation, secondBinding, map[string]string{
		"TEST_FAIL_BEFORE_READY": "forced peer failure before ready",
		"TEST_REVIEWER_PID":      secondPID,
		"TEST_SYNC_FDS":          "1",
	})
	result, err := testenv.RunSynchronized(t, reviewLoopFixtureTimeout,
		testenv.SynchronizedCommand{Name: "waiting peer", Command: first},
		testenv.SynchronizedCommand{Name: "failed peer", Command: second},
	)
	require.Error(t, err)
	require.Less(t, result.Duration, 30*time.Second, err.Error())
	t.Logf("bounded peer failure completed in %s", result.Duration)
	require.Error(t, result.Exit["waiting peer"])
	_, failedPeerExited := result.Exit["failed peer"]
	require.True(t, failedPeerExited, "the failed review-loop process must be reaped: %v", err)
	require.Contains(t, err.Error(), "failed peer failed before all peers were ready")
	require.Contains(t, err.Error(), "forced peer failure before ready")
	require.Contains(t, err.Error(), "waiting peer stdout/stderr:")
	require.Contains(t, err.Error(), "failed peer stdout/stderr:")
	requireProcessGone(t, firstPID)
	requireProcessGone(t, secondPID)
}

func newWorktreeBindingFixture(t *testing.T) worktreeBindingFixture {
	t.Helper()

	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	require.NoError(t, os.MkdirAll(root, 0o755))
	runGit(t, root, "init")
	runGit(t, root, "config", "user.name", "Worktree Binding Test")
	runGit(t, root, "config", "user.email", "worktree-binding@example.com")
	require.NoError(t, os.WriteFile(filepath.Join(root, "base.txt"), []byte("base\n"), 0o644))
	runGit(t, root, "add", "base.txt")
	runGit(t, root, "commit", "-m", "base")
	head := strings.TrimSpace(runGitOutput(t, root, "rev-parse", "HEAD"))
	candidate := filepath.Join(parent, "candidate-123")
	runGit(t, root, "worktree", "add", "--detach", candidate, head)
	validationDir := filepath.Join(parent, "validation-123")
	require.NoError(t, os.MkdirAll(validationDir, 0o755))
	bindingFile := filepath.Join(parent, "binding-123.json")
	writeBindingFile(t, bindingFile, 123, candidate, head, root)

	reviewer := filepath.Join(parent, "reviewer.sh")
	require.NoError(t, os.WriteFile(reviewer, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ -n "${TEST_REVIEWER_PID:-}" ]]; then
    printf '%s\n' "$$" > "$TEST_REVIEWER_PID"
fi
if [[ -n "${TEST_FAIL_BEFORE_READY:-}" ]]; then
    printf '%s\n' "$TEST_FAIL_BEFORE_READY" >&2
    exit 77
fi
if [[ "${TEST_SYNC_FDS:-}" == 1 ]]; then
    printf 'ready\n' >&3
    IFS= read -r _ <&4
fi
if [[ -n "${TEST_AGENT_CWD:-}" ]]; then pwd > "$TEST_AGENT_CWD"; fi
case "${TEST_ROOT_MUTATION:-}" in
    branch)
        mkdir -p "$TRUSTED_ROOT_CHECKOUT/.git/refs/heads"
        printf '%s\n' "$EXPECTED_HEAD" > "$TRUSTED_ROOT_CHECKOUT/.git/refs/heads/agent-mutated-root"
        printf 'ref: refs/heads/agent-mutated-root\n' > "$TRUSTED_ROOT_CHECKOUT/.git/HEAD"
        ;;
    untracked) printf 'mutation\n' > "$TRUSTED_ROOT_CHECKOUT/agent-root-mutation.txt" ;;
    dirty-content) printf 'different dirty content\n' > "$TRUSTED_ROOT_CHECKOUT/base.txt" ;;
    ignored-content) printf 'different ignored content\n' > "$TRUSTED_ROOT_CHECKOUT/ignored-secret.txt" ;;
    guarded-branch) git -C "$TRUSTED_ROOT_CHECKOUT" switch -c guard-must-refuse ;;
    guard-stash) git -C "$TRUSTED_ROOT_CHECKOUT" stash ;;
    guard-apply) git -C "$TRUSTED_ROOT_CHECKOUT" apply ;;
    guard-revert) git -C "$TRUSTED_ROOT_CHECKOUT" revert "$EXPECTED_HEAD" ;;
    guard-am) git -C "$TRUSTED_ROOT_CHECKOUT" am ;;
    guard-output) git -C "$TRUSTED_ROOT_CHECKOUT" diff --output=base.txt ;;
    child-worktree) git worktree add --detach "$(dirname "$EXPECTED_WORKTREE")/agent-child" "$EXPECTED_HEAD" ;;
    fixture-worktree)
        fixture_repo="$VALIDATION_RUN_DIR/fixture-repo"
        fixture_child="$VALIDATION_RUN_DIR/fixture-child"
        mkdir -p "$fixture_repo"
        git -C "$fixture_repo" init -q
        git -C "$fixture_repo" config user.name test
        git -C "$fixture_repo" config user.email test@example.com
        printf 'fixture\n' > "$fixture_repo/file.txt"
        git -C "$fixture_repo" add file.txt
        git -C "$fixture_repo" commit -qm fixture
        git -C "$fixture_repo" worktree add -q --detach "$fixture_child" HEAD
        ;;
    guard-environment)
        [[ ${AI_GIT_REAL_PATH+x} != x ]]
        [[ ${AI_GIT_ORIGINAL_PATH+x} != x ]]
        ;;
    direct-git-bypass)
        "$TEST_UNGUARDED_GIT" -C "$TRUSTED_ROOT_CHECKOUT" switch -c direct-bypass-must-be-detected
        ;;
    guard-read) git -C "$TRUSTED_ROOT_CHECKOUT" status --short >/dev/null ;;
esac
printf '{"decision":"APPROVE","head":"%s","worktree_fingerprint":"%s","known_findings":[],"findings":[],"residual_risk":"LOW","human_decision_context":""}\n' \
    "$AI_REVIEW_HEAD" "$AI_REVIEW_WORKTREE_FINGERPRINT" > "$AI_REVIEW_RESULT"
`), 0o755))

	binary := filepath.Join(parent, "review-loop")
	build := testenv.Command(t, "go", "build", "-o", binary, ".")
	build.Dir = filepath.Join(repositoryRootForTests(t), "scripts", "reviewloop")
	output, err := build.CombinedOutput()
	require.NoError(t, err, string(output))

	return worktreeBindingFixture{
		root:          root,
		candidate:     candidate,
		validationDir: validationDir,
		bindingFile:   bindingFile,
		head:          head,
		binary:        binary,
		reviewer:      reviewer,
		guard:         filepath.Join(repositoryRootForTests(t), "scripts", "ai-git-guard"),
	}
}

func (fixture worktreeBindingFixture) run(
	t *testing.T,
	prNumber int,
	candidate string,
	bindingFile string,
	extraEnvironment map[string]string,
) (string, error) {
	t.Helper()

	command := fixture.command(prNumber, candidate, fixture.validationDir, bindingFile, extraEnvironment)
	output, err := command.CombinedOutput()

	return string(output), err
}

func (fixture worktreeBindingFixture) command(
	prNumber int,
	candidate string,
	validationDir string,
	bindingFile string,
	extraEnvironment map[string]string,
) *exec.Cmd {
	arguments := []string{
		"--base", fixture.head,
		"--pr", strconv.Itoa(prNumber),
		"--worktree", candidate,
		"--expected-head", fixture.head,
		"--trusted-root", fixture.root,
		"--binding-file", bindingFile,
		"--validation-run-dir", validationDir,
		"--git-guard", fixture.guard,
		"--review-cmd", "bash " + shellQuote(fixture.reviewer),
		"--validation-cmd", "true",
		"--state-dir", filepath.Join(candidate, ".review-state"),
	}
	command := exec.Command(fixture.binary, arguments...)
	command.Dir = candidate
	command.Env = testenv.EnvironmentMap(extraEnvironment)

	return command
}

func requireProcessGone(t *testing.T, pidFile string) {
	t.Helper()
	pidText := strings.TrimSpace(readTestFile(t, pidFile))
	pid, err := strconv.Atoi(pidText)
	require.NoError(t, err)
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		err = syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		select {
		case <-deadline.C:
			require.ErrorIs(t, err, syscall.ESRCH, "reviewer process %d is still alive", pid)

			return
		case <-ticker.C:
		}
	}
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()

	content, err := os.ReadFile(path)
	require.NoError(t, err)

	return string(content)
}

func canonicalTestPath(t *testing.T, path string) string {
	t.Helper()

	resolved, err := filepath.EvalSymlinks(path)
	require.NoError(t, err)

	return resolved
}
