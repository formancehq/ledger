package main

import (
	"errors"
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

const reviewLoopFixtureTimeout = 45 * time.Second

type worktreeIdentityFixture struct {
	root          string
	candidate     string
	validationDir string
	head          string
	binary        string
	reviewer      string
}

func TestReviewFlowUsesExpectedIdentityContract(t *testing.T) {
	fixture := newWorktreeIdentityFixture(t)
	capture := filepath.Join(filepath.Dir(fixture.candidate), "identity")

	output, err := fixture.run(t, fixture.candidate, fixture.head, map[string]string{
		"EXPECTED_PR_NUMBER": "999999",
		"EXPECTED_WORKTREE":  "/inherited/wrong-worktree",
		"EXPECTED_HEAD":      strings.Repeat("f", 40),
		"TEST_CAPTURE":       capture,
	})
	require.NoError(t, err, output)
	require.Contains(t, output, "WORKTREE_BINDING_GATE=PASS role=review")
	require.Equal(t, "123|"+canonicalTestPath(t, fixture.candidate)+"|"+fixture.head, strings.TrimSpace(readTestFile(t, capture)))
}

func TestCandidateModificationsAreAllowed(t *testing.T) {
	fixture := newWorktreeIdentityFixture(t)
	require.NoError(t, os.WriteFile(filepath.Join(fixture.candidate, "candidate-change.txt"), []byte("candidate\n"), 0o644))

	output, err := fixture.run(t, fixture.candidate, fixture.head, nil)
	require.NoError(t, err, output)
	require.Contains(t, output, "REVIEW_LOOP_RESULT: APPROVE")
}

func TestIdentityRejectsWrongCandidate(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*testing.T, *worktreeIdentityFixture) (string, string, string)
		want      string
	}{
		{
			name: "cwd",
			configure: func(_ *testing.T, fixture *worktreeIdentityFixture) (string, string, string) {
				return fixture.candidate, fixture.head, fixture.root
			},
			want: "launcher cwd is",
		},
		{
			name: "HEAD",
			configure: func(_ *testing.T, fixture *worktreeIdentityFixture) (string, string, string) {
				return fixture.candidate, strings.Repeat("f", 40), fixture.candidate
			},
			want: "candidate HEAD is",
		},
		{
			name: "repository",
			configure: func(t *testing.T, fixture *worktreeIdentityFixture) (string, string, string) {
				other := newIndependentRepository(t)
				head := strings.TrimSpace(runGitOutput(t, other, "rev-parse", "HEAD"))

				return other, head, other
			},
			want: "same Git worktree set",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newWorktreeIdentityFixture(t)
			candidate, head, cwd := test.configure(t, fixture)
			command := fixture.command(candidate, head, nil)
			command.Dir = cwd
			output, err := command.CombinedOutput()
			require.Error(t, err, string(output))
			require.Contains(t, string(output), test.want)
			require.NotContains(t, string(output), "WORKTREE_BINDING_GATE=PASS")
		})
	}
}

func TestIdentityRejectsPrimaryCheckoutAsCandidate(t *testing.T) {
	fixture := newWorktreeIdentityFixture(t)
	command := fixture.command(fixture.root, fixture.head, nil)
	command.Dir = fixture.root
	output, err := command.CombinedOutput()
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "ROOT_CHECKOUT_AS_CANDIDATE_FORBIDDEN")
}

func TestNestedPeerFailureIsBoundedAndReaped(t *testing.T) {
	fixture := newWorktreeIdentityFixture(t)
	parent := filepath.Dir(fixture.candidate)
	secondCandidate := filepath.Join(parent, "candidate-456")
	runGit(t, fixture.root, "worktree", "add", "--detach", secondCandidate, fixture.head)
	secondValidation := filepath.Join(parent, "validation-456")
	require.NoError(t, os.MkdirAll(secondValidation, 0o755))
	firstPID := filepath.Join(parent, "reviewer-123.pid")
	secondPID := filepath.Join(parent, "reviewer-456.pid")

	first := fixture.commandFor(123, fixture.candidate, fixture.head, fixture.validationDir, map[string]string{
		"TEST_REVIEWER_PID": firstPID,
		"TEST_SYNC_FDS":     "1",
	})
	second := fixture.commandFor(456, secondCandidate, fixture.head, secondValidation, map[string]string{
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
	require.Contains(t, err.Error(), "failed peer failed before all peers were ready")
	require.Contains(t, err.Error(), "forced peer failure before ready")
	require.Contains(t, err.Error(), "waiting peer stdout/stderr:")
	require.Contains(t, err.Error(), "failed peer stdout/stderr:")
	requireProcessGone(t, firstPID)
	requireProcessGone(t, secondPID)
}

func newWorktreeIdentityFixture(t *testing.T) *worktreeIdentityFixture {
	t.Helper()

	parent := t.TempDir()
	root := newIndependentRepositoryAt(t, filepath.Join(parent, "root"))
	head := strings.TrimSpace(runGitOutput(t, root, "rev-parse", "HEAD"))
	candidate := filepath.Join(parent, "candidate-123")
	runGit(t, root, "worktree", "add", "--detach", candidate, head)
	validationDir := filepath.Join(parent, "validation-123")
	require.NoError(t, os.MkdirAll(validationDir, 0o755))

	reviewer := filepath.Join(parent, "reviewer.sh")
	require.NoError(t, os.WriteFile(reviewer, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ -n "${TEST_REVIEWER_PID:-}" ]]; then printf '%s\n' "$$" > "$TEST_REVIEWER_PID"; fi
if [[ -n "${TEST_FAIL_BEFORE_READY:-}" ]]; then printf '%s\n' "$TEST_FAIL_BEFORE_READY" >&2; exit 77; fi
if [[ "${TEST_SYNC_FDS:-}" == 1 ]]; then printf 'ready\n' >&3; IFS= read -r _ <&4; fi
if [[ -n "${TEST_CAPTURE:-}" ]]; then
    printf '%s|%s|%s\n' "$EXPECTED_PR_NUMBER" "$EXPECTED_WORKTREE" "$EXPECTED_HEAD" > "$TEST_CAPTURE"
fi
printf '{"decision":"APPROVE","head":"%s","worktree_fingerprint":"%s","known_findings":[],"findings":[],"residual_risk":"LOW","human_decision_context":""}\n' \
    "$AI_REVIEW_HEAD" "$AI_REVIEW_WORKTREE_FINGERPRINT" > "$AI_REVIEW_RESULT"
`), 0o755))

	binary := filepath.Join(parent, "review-loop")
	build := testenv.Command(t, "go", "build", "-o", binary, ".")
	build.Dir = filepath.Join(repositoryRootForTests(t), "scripts", "reviewloop")
	output, err := build.CombinedOutput()
	require.NoError(t, err, string(output))

	return &worktreeIdentityFixture{
		root:          root,
		candidate:     candidate,
		validationDir: validationDir,
		head:          head,
		binary:        binary,
		reviewer:      reviewer,
	}
}

func (fixture *worktreeIdentityFixture) run(t *testing.T, candidate, head string, environment map[string]string) (string, error) {
	t.Helper()
	output, err := fixture.command(candidate, head, environment).CombinedOutput()

	return string(output), err
}

func (fixture *worktreeIdentityFixture) command(candidate, head string, environment map[string]string) *exec.Cmd {
	return fixture.commandFor(123, candidate, head, fixture.validationDir, environment)
}

func (fixture *worktreeIdentityFixture) commandFor(pr int, candidate, head, validationDir string, environment map[string]string) *exec.Cmd {
	command := exec.Command(fixture.binary,
		"--base", fixture.head,
		"--pr", strconv.Itoa(pr),
		"--worktree", candidate,
		"--expected-head", head,
		"--trusted-root", fixture.root,
		"--validation-run-dir", validationDir,
		"--review-cmd", "bash "+shellQuote(fixture.reviewer),
		"--validation-cmd", "true",
		"--state-dir", filepath.Join(validationDir, "review-state"),
	)
	command.Dir = candidate
	command.Env = testenv.EnvironmentMap(environment)

	return command
}

func newIndependentRepository(t *testing.T) string {
	t.Helper()

	return newIndependentRepositoryAt(t, filepath.Join(t.TempDir(), "repository"))
}

func newIndependentRepositoryAt(t *testing.T, root string) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(root, 0o755))
	runGit(t, root, "init")
	require.NoError(t, os.WriteFile(filepath.Join(root, "base.txt"), []byte("base\n"), 0o644))
	runGit(t, root, "add", "base.txt")
	runGit(t, root, "-c", "user.name=Review Loop Test", "-c", "user.email=review-loop@example.com", "commit", "-m", "base")

	return root
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
