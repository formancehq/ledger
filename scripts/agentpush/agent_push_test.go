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
	bin := filepath.Join(dir, "bin")
	require.NoError(t, os.Mkdir(bin, 0o755))
	ghScript := "#!/usr/bin/env bash\nset -euo pipefail\n[[ $* == *'baseRefOid'* ]]\nprintf '%s\\n' \"$EXPECTED_PR_BASE\"\n"
	require.NoError(t, os.WriteFile(filepath.Join(bin, "gh"), []byte(ghScript), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "tracked.txt"), []byte("initial\n"), 0o644))
	run(t, repo, nil, "git", "add", ".")
	run(t, repo, nil, "git", "commit", "-m", "initial")
	run(t, repo, nil, "git", "push", "origin", "HEAD:refs/heads/chore/ci-local-format-parity")
	initialRemote := run(t, remote, nil, "git", "rev-parse", "refs/heads/chore/ci-local-format-parity")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "tracked.txt"), []byte("candidate\n"), 0o644))
	run(t, repo, nil, "git", "add", "tracked.txt")
	run(t, repo, nil, "git", "commit", "-m", "candidate")
	base := run(t, repo, nil, "git", "rev-parse", "HEAD^")
	baseEnv := []string{"AI_REVIEW_BASE_SHA=" + base, "EXPECTED_PR_BASE=" + base, "PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH")}
	output, err := runResult(repo, baseEnv, "bash", "scripts/agent-push", "origin", "chore/another-branch")
	require.Error(t, err)
	require.Contains(t, output, "does not match push target")
	require.Equal(t, initialRemote, run(t, remote, nil, "git", "rev-parse", "refs/heads/chore/ci-local-format-parity"))

	output, err = runResult(repo, append(baseEnv, "AI_REVIEW_BASE_SHA="), "bash", "scripts/agent-push", "origin", "chore/ci-local-format-parity")
	require.Error(t, err)
	require.Contains(t, output, "AI_REVIEW_BASE_SHA")
	require.Equal(t, initialRemote, run(t, remote, nil, "git", "rev-parse", "refs/heads/chore/ci-local-format-parity"))
	output, err = runResult(repo, append(baseEnv, "AI_REVIEW_BASE_SHA="+run(t, repo, nil, "git", "rev-parse", "HEAD")), "bash", "scripts/agent-push", "origin", "chore/ci-local-format-parity")
	require.Error(t, err)
	require.Contains(t, output, "does not match PR base")
	require.Equal(t, initialRemote, run(t, remote, nil, "git", "rev-parse", "refs/heads/chore/ci-local-format-parity"))

	_, err = runResult(repo, append(baseEnv, "VALIDATOR_MODE=fail"), "bash", "scripts/agent-push", "origin", "chore/ci-local-format-parity")
	require.Error(t, err)
	require.Equal(t, initialRemote, run(t, remote, nil, "git", "rev-parse", "refs/heads/chore/ci-local-format-parity"))

	output, err = runResult(repo, append(baseEnv, "VALIDATOR_MODE=dirty"), "bash", "scripts/agent-push", "origin", "chore/ci-local-format-parity")
	require.Error(t, err)
	require.Contains(t, output, "validation changed the worktree")
	require.Equal(t, initialRemote, run(t, remote, nil, "git", "rev-parse", "refs/heads/chore/ci-local-format-parity"))
	run(t, repo, nil, "git", "restore", "tracked.txt")

	realGit, err := exec.LookPath("git")
	require.NoError(t, err)
	pushArgsLog := filepath.Join(dir, "push-args")
	gitShim := "#!/usr/bin/env bash\nset -euo pipefail\nif [[ ${1:-} == push ]]; then printf '%s' \"$3\" > \"$PUSH_ARGS_LOG\"; fi\nPATH=\"${PATH#\"$SHIM_BIN:\"}\" exec \"$REAL_GIT\" \"$@\"\n"
	require.NoError(t, os.WriteFile(filepath.Join(bin, "git"), []byte(gitShim), 0o755))
	validatedHead := run(t, repo, nil, "git", "rev-parse", "HEAD")
	pushEnv := []string{
		"AI_REVIEW_BASE_SHA=" + base,
		"VALIDATOR_MODE=pass",
		"REAL_GIT=" + realGit,
		"PUSH_ARGS_LOG=" + pushArgsLog,
		"SHIM_BIN=" + bin,
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"EXPECTED_PR_BASE=" + base,
	}
	run(t, repo, pushEnv, "bash", "scripts/agent-push", "origin", "chore/ci-local-format-parity")
	refspec, err := os.ReadFile(pushArgsLog)
	require.NoError(t, err)
	require.Equal(t, validatedHead+":refs/heads/chore/ci-local-format-parity", string(refspec))
	require.Equal(t, validatedHead, run(t, remote, nil, "git", "rev-parse", "refs/heads/chore/ci-local-format-parity"))
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
	command.Env = mergeEnvironment(os.Environ(), env)
	output, err := command.CombinedOutput()

	return string(output), err
}

func mergeEnvironment(current, overrides []string) []string {
	merged := make(map[string]string, len(current)+len(overrides))
	for _, entry := range current {
		key, value, _ := strings.Cut(entry, "=")
		merged[key] = value
	}
	for _, entry := range overrides {
		key, value, _ := strings.Cut(entry, "=")
		merged[key] = value
	}
	result := make([]string, 0, len(merged))
	for key, value := range merged {
		result = append(result, key+"="+value)
	}

	return result
}
