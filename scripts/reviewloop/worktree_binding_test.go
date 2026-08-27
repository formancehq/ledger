package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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

func TestGitMutationGuardAllowsReadOnlyRootInspection(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)

	output, err := fixture.run(t, 123, fixture.candidate, fixture.bindingFile, map[string]string{
		"TEST_ROOT_MUTATION": "guard-read",
	})
	require.NoError(t, err, output)
	require.Contains(t, output, "ROOT_UNCHANGED=PASS")
}

func TestConcurrentPRRunsUseDistinctWorktreesAndValidationDirs(t *testing.T) {
	fixture := newWorktreeBindingFixture(t)
	parent := filepath.Dir(fixture.candidate)
	secondCandidate := filepath.Join(parent, "candidate-456")
	runGit(t, fixture.root, "worktree", "add", "--detach", secondCandidate, fixture.head)
	secondValidation := filepath.Join(parent, "validation-456")
	require.NoError(t, os.MkdirAll(secondValidation, 0o755))
	secondBinding := filepath.Join(parent, "binding-456.json")
	writeBindingFile(t, secondBinding, 456, secondCandidate, fixture.head, fixture.root)

	firstReady := filepath.Join(parent, "ready-123")
	secondReady := filepath.Join(parent, "ready-456")
	firstCWD := filepath.Join(parent, "cwd-123")
	secondCWD := filepath.Join(parent, "cwd-456")
	first := fixture.command(123, fixture.candidate, fixture.validationDir, fixture.bindingFile, map[string]string{
		"TEST_AGENT_CWD":   firstCWD,
		"TEST_READY":       firstReady,
		"TEST_OTHER_READY": secondReady,
	})
	second := fixture.command(456, secondCandidate, secondValidation, secondBinding, map[string]string{
		"TEST_AGENT_CWD":   secondCWD,
		"TEST_READY":       secondReady,
		"TEST_OTHER_READY": firstReady,
	})
	var firstOutput, secondOutput bytes.Buffer
	first.Stdout, first.Stderr = &firstOutput, &firstOutput
	second.Stdout, second.Stderr = &secondOutput, &secondOutput
	require.NoError(t, first.Start())
	require.NoError(t, second.Start())
	require.NoError(t, first.Wait(), firstOutput.String())
	require.NoError(t, second.Wait(), secondOutput.String())

	require.NotEqual(t, fixture.candidate, secondCandidate)
	require.NotEqual(t, fixture.validationDir, secondValidation)
	require.NotEqual(t, fixture.candidate, fixture.validationDir)
	require.NotEqual(t, secondCandidate, secondValidation)
	require.Equal(t, canonicalTestPath(t, fixture.candidate), strings.TrimSpace(readTestFile(t, firstCWD)))
	require.Equal(t, canonicalTestPath(t, secondCandidate), strings.TrimSpace(readTestFile(t, secondCWD)))
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
if [[ -n "${TEST_READY:-}" ]]; then
    : > "$TEST_READY"
    while [[ ! -e "$TEST_OTHER_READY" ]]; do :; done
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
    guard-read) git -C "$TRUSTED_ROOT_CHECKOUT" status --short >/dev/null ;;
esac
printf '{"decision":"APPROVE","head":"%s","worktree_fingerprint":"%s","previous_findings":[],"findings":[],"residual_risk":"LOW","human_decision_context":""}\n' \
    "$AI_REVIEW_HEAD" "$AI_REVIEW_WORKTREE_FINGERPRINT" > "$AI_REVIEW_RESULT"
`), 0o755))

	binary := filepath.Join(parent, "review-loop")
	build := exec.Command("go", "build", "-o", binary, ".")
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
	command.Env = replaceEnvironment(os.Environ(), extraEnvironment)

	return command
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
