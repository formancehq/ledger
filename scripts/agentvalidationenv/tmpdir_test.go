package agentvalidationenv

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/scripts/internal/testenv"
)

func TestTmpDirIsInheritedUnchanged(t *testing.T) {
	t.Parallel()

	root, repository := newTmpDirFixture(t)
	target := filepath.Join(root, "temporary")
	require.NoError(t, os.MkdirAll(target, 0o755))
	// A symlinked path must be exported as given, not canonicalized: the value
	// itself is part of Go's test cache key.
	alias := filepath.Join(root, "temporary-alias")
	require.NoError(t, os.Symlink(target, alias))

	for _, mode := range []string{"direct", "--ephemeral"} {
		output, err := tmpDirCommand(t, repository, root, mode, "TMPDIR="+alias).CombinedOutput()
		require.NoError(t, err, string(output))
		require.Equal(t, alias, string(output), mode)
	}
}

func TestTmpDirDefaultsWhenUnset(t *testing.T) {
	t.Parallel()

	root, repository := newTmpDirFixture(t)
	output, err := tmpDirCommand(t, repository, root, "direct", "TMPDIR=").CombinedOutput()
	require.NoError(t, err, string(output))
	require.Equal(t, "/tmp", string(output))
}

func TestRejectsUnsafeTmpDir(t *testing.T) {
	t.Parallel()

	root, repository := newTmpDirFixture(t)
	inside := filepath.Join(repository, "tmp")
	require.NoError(t, os.MkdirAll(inside, 0o755))
	alias := filepath.Join(root, "inside-alias")
	require.NoError(t, os.Symlink(inside, alias))

	for _, tc := range []struct{ name, mode, tmpdir string }{
		{"inside candidate", "direct", inside},
		{"symlink into candidate", "direct", alias},
		{"candidate root", "direct", repository},
		{"relative", "direct", "relative-tmp"},
		{"missing", "direct", filepath.Join(root, "missing")},
		{"inside candidate", "--ephemeral", inside},
	} {
		output, err := tmpDirCommand(t, repository, root, tc.mode, "TMPDIR="+tc.tmpdir).CombinedOutput()
		require.Error(t, err, tc.name+" "+tc.mode)
		require.Contains(t, string(output), "TMPDIR must be an absolute existing directory outside the candidate worktree", tc.name+" "+tc.mode)
	}
	// The ephemeral run directory is created inside TMPDIR, so the guard must
	// reject before anything is written into the candidate.
	entries, err := os.ReadDir(inside)
	require.NoError(t, err)
	require.Empty(t, entries)
}

// A caller that sets VALIDATION_RUN_DIR itself skips the wrapper's TMPDIR
// check, so agent-check-pr must reject an in-worktree TMPDIR on its own.
func TestAgentCheckPRSharedCacheGateRejectsUnsafeTmpDir(t *testing.T) {
	t.Parallel()

	root, repository := newTmpDirFixture(t)
	runGit(t, repository, "-c", "user.name=Agent Check PR Test", "-c", "user.email=agent-check-pr@example.com",
		"-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "base")
	baseSHA := strings.TrimSpace(gitOutput(t, repository, "rev-parse", "HEAD"))
	inside := filepath.Join(repository, "tmp")
	require.NoError(t, os.MkdirAll(inside, 0o755))
	alias := filepath.Join(root, "inside-alias")
	require.NoError(t, os.Symlink(inside, alias))
	outside := filepath.Join(root, "temporary")
	require.NoError(t, os.MkdirAll(outside, 0o755))

	// Run a verbatim copy of agent-check-pr with a stubbed toolchain gate, so
	// the test reaches SHARED_CACHE_GATE without a Nix-provided Go.
	scripts := filepath.Join(root, "scripts")
	require.NoError(t, os.MkdirAll(scripts, 0o755))
	for _, name := range []string{"agent-check-pr", "agent-cache-contract"} {
		contents, err := os.ReadFile(filepath.Join(filepath.Dir(validationEnvPath(t)), name))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(scripts, name), contents, 0o755))
	}
	require.NoError(t, os.WriteFile(filepath.Join(scripts, "ai-bugfix-gate"), []byte("#!/usr/bin/env bash\n"), 0o755))

	for _, tc := range []struct {
		name, tmpdir string
		accepted     bool
	}{
		{"outside candidate", outside, true},
		{"inside candidate", inside, false},
		{"symlink into candidate", alias, false},
		{"candidate root", repository, false},
		{"relative", "relative-tmp", false},
	} {
		// The wrapper validates the caller's TMPDIR and sets up the shared
		// caches; the override below only reaches agent-check-pr.
		command := testenv.Command(t, "bash", validationEnvPath(t), filepath.Join(root, "run"),
			"env", "TMPDIR="+tc.tmpdir, "bash", filepath.Join(scripts, "agent-check-pr"), "--normalize")
		command.Dir = repository
		command.Env = testenv.Environment(
			"HOME="+root,
			"LEDGER_AI_CACHE_ROOT="+filepath.Join(root, "shared-cache"),
			"TMPDIR="+outside,
			"AI_REVIEW_BASE_SHA="+baseSHA,
			"LEDGER_AGENT_CHECK_IN_NIX=1",
		)
		output, err := command.CombinedOutput()
		if tc.accepted {
			require.NoError(t, err, "%s: %s", tc.name, output)
			require.Contains(t, string(output), "SHARED_CACHE_GATE=PASS", tc.name)

			continue
		}
		require.Error(t, err, "%s: %s", tc.name, output)
		require.Contains(t, string(output), "SHARED_CACHE_GATE=FAIL (TMPDIR is not an absolute directory outside the candidate worktree)", tc.name)
	}
}

func gitOutput(t *testing.T, directory string, arguments ...string) string {
	t.Helper()
	command := testenv.Command(t, "git", arguments...)
	command.Dir = directory
	output, err := command.Output()
	require.NoError(t, err)

	return string(output)
}

func newTmpDirFixture(t *testing.T) (string, string) {
	t.Helper()

	root := resolvedPath(t, t.TempDir())
	repository := filepath.Join(root, "candidate")
	require.NoError(t, os.MkdirAll(repository, 0o755))
	runGit(t, repository, "init")

	return root, repository
}

func tmpDirCommand(t *testing.T, repository, root, mode, tmpdir string) *exec.Cmd {
	t.Helper()

	arguments := []string{validationEnvPath(t)}
	if mode == "--ephemeral" {
		arguments = append(arguments, "--ephemeral")
	} else {
		arguments = append(arguments, filepath.Join(root, "run"))
	}
	arguments = append(arguments, "sh", "-c", `printf %s "$TMPDIR"`)
	command := testenv.Command(t, "bash", arguments...)
	command.Dir = repository
	command.Env = testenv.Environment("HOME="+root, "LEDGER_AI_CACHE_ROOT="+filepath.Join(root, "shared-cache"), tmpdir)

	return command
}

// Validation shares one TMPDIR across concurrent runs, and golangci-lint locks
// $TMPDIR/golangci-lint.lock unless parallel runners are allowed. Without the
// flag, a run gives up after 5s with "parallel golangci-lint is running".
func TestLintRunsWhileAnotherRunHoldsTheTmpDirLock(t *testing.T) {
	t.Parallel()

	seed, repository, _, cacheRoot := newLintWorktrees(t)
	sharedTmp := filepath.Join(filepath.Dir(seed), "shared-tmp")
	require.NoError(t, os.MkdirAll(sharedTmp, 0o755))
	lock, err := os.Create(filepath.Join(sharedTmp, "golangci-lint.lock"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = lock.Close() })
	require.NoError(t, syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))

	command, output := lintCommand(t, repository, cacheRoot, filepath.Join(filepath.Dir(seed), "run"))
	command.Env = append(command.Env, "TMPDIR="+sharedTmp)
	require.NoError(t, command.Run(), output.String())
	require.Contains(t, output.String(), "x.go:4:2: ineffectual assignment")
}

func TestJustfileLintAllowsParallelRunners(t *testing.T) {
	t.Parallel()

	justfile, err := os.ReadFile(filepath.Join("..", "..", "justfile"))
	require.NoError(t, err)
	var invocations int
	for line := range strings.SplitSeq(string(justfile), "\n") {
		if !strings.Contains(line, "golangci-lint run") {
			continue
		}
		invocations++
		require.Contains(t, line, "--allow-parallel-runners", "justfile lint invocation can fail on a shared TMPDIR: %s", strings.TrimSpace(line))
	}
	require.Equal(t, 3, invocations)
}
