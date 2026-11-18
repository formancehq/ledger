package agentpush

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAgentPushRequiresPassingCleanValidation(t *testing.T) {
	dir := t.TempDir()
	repo := filepath.Join(dir, "repo")
	remote := filepath.Join(dir, "remote.git")
	require.NoError(t, os.Mkdir(repo, 0o755))
	run(t, dir, nil, "git", "init", "--bare", remote)
	run(t, repo, nil, "git", "init", "-b", "chore/ci-local-format-parity")
	run(t, repo, nil, "git", "config", "user.name", "Agent Push Test")
	run(t, repo, nil, "git", "config", "user.email", "agent-push@example.com")
	run(t, repo, nil, "git", "remote", "add", "origin", remote)
	require.NoError(t, os.Mkdir(filepath.Join(repo, "scripts"), 0o755))
	pushScript, err := os.ReadFile(filepath.Join("..", "agent-push"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(repo, "scripts", "agent-push"), pushScript, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "scripts", "agent-check-pr"), []byte(`#!/usr/bin/env bash
set -euo pipefail
case "${VALIDATOR_MODE:-pass}" in
    fail) exit 17 ;;
    dirty) printf 'normalized\n' >> tracked.txt ;;
esac
`), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "tracked.txt"), []byte("initial\n"), 0o644))
	run(t, repo, nil, "git", "add", ".")
	run(t, repo, nil, "git", "commit", "-m", "initial")
	run(t, repo, nil, "git", "push", "origin", "HEAD:refs/heads/chore/ci-local-format-parity")
	initialRemote := run(t, remote, nil, "git", "rev-parse", "refs/heads/chore/ci-local-format-parity")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "tracked.txt"), []byte("candidate\n"), 0o644))
	run(t, repo, nil, "git", "add", "tracked.txt")
	run(t, repo, nil, "git", "commit", "-m", "candidate")
	base := run(t, repo, nil, "git", "rev-parse", "HEAD^")
	output, err := runResult(repo, []string{"AI_REVIEW_BASE_SHA=" + base}, "bash", "scripts/agent-push", "origin", "chore/another-branch")
	require.Error(t, err)
	require.Contains(t, output, "does not match push target")
	require.Equal(t, initialRemote, run(t, remote, nil, "git", "rev-parse", "refs/heads/chore/ci-local-format-parity"))

	output, err = runResult(repo, []string{"AI_REVIEW_BASE_SHA="}, "bash", "scripts/agent-push", "origin", "chore/ci-local-format-parity")
	require.Error(t, err)
	require.Contains(t, output, "AI_REVIEW_BASE_SHA")
	require.Equal(t, initialRemote, run(t, remote, nil, "git", "rev-parse", "refs/heads/chore/ci-local-format-parity"))

	_, err = runResult(repo, []string{"AI_REVIEW_BASE_SHA=" + base, "VALIDATOR_MODE=fail"}, "bash", "scripts/agent-push", "origin", "chore/ci-local-format-parity")
	require.Error(t, err)
	require.Equal(t, initialRemote, run(t, remote, nil, "git", "rev-parse", "refs/heads/chore/ci-local-format-parity"))

	output, err = runResult(repo, []string{"AI_REVIEW_BASE_SHA=" + base, "VALIDATOR_MODE=dirty"}, "bash", "scripts/agent-push", "origin", "chore/ci-local-format-parity")
	require.Error(t, err)
	require.Contains(t, output, "validation changed the worktree")
	require.Equal(t, initialRemote, run(t, remote, nil, "git", "rev-parse", "refs/heads/chore/ci-local-format-parity"))
	run(t, repo, nil, "git", "restore", "tracked.txt")

	run(t, repo, []string{"AI_REVIEW_BASE_SHA=" + base, "VALIDATOR_MODE=pass"}, "bash", "scripts/agent-push", "origin", "chore/ci-local-format-parity")
	require.Equal(t, run(t, repo, nil, "git", "rev-parse", "HEAD"), run(t, remote, nil, "git", "rev-parse", "refs/heads/chore/ci-local-format-parity"))
}

func run(t *testing.T, dir string, env []string, name string, args ...string) string {
	t.Helper()
	output, err := runResult(dir, env, name, args...)
	require.NoError(t, err, output)

	return strings.TrimSpace(output)
}

func runResult(dir string, env []string, name string, args ...string) (string, error) {
	command := exec.Command(name, args...)
	command.Dir = dir
	command.Env = append(os.Environ(), env...)
	output, err := command.CombinedOutput()

	return string(output), err
}
