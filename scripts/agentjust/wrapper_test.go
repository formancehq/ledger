package agentjust

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWrapperPinsJustfileAndUsesReviewedWorktree(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	toolRoot := filepath.Join(root, "trusted-tools")
	worktree := filepath.Join(root, "reviewed-worktree")
	for _, directory := range []string{filepath.Join(toolRoot, "scripts"), worktree} {
		require.NoError(t, os.MkdirAll(directory, 0o755))
	}

	wrapper, err := os.ReadFile(wrapperPath(t))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(toolRoot, "scripts", "agent-just"), wrapper, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(toolRoot, "justfile"), []byte("trusted:\n    printf 'trusted\\n' > \"$TEST_RESULT\"\n    pwd > \"$TEST_WORKING_DIRECTORY\"\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(worktree, "justfile"), []byte("trusted:\n    printf 'target-controlled\\n' > \"$TEST_RESULT\"\n"), 0o644))
	runGit(t, worktree, "init")
	resolvedWorktree, err := filepath.EvalSymlinks(worktree)
	require.NoError(t, err)

	resultPath := filepath.Join(root, "result")
	workingDirectoryPath := filepath.Join(root, "working-directory")
	command := exec.Command("bash", filepath.Join(toolRoot, "scripts", "agent-just"), "trusted")
	command.Dir = worktree
	command.Env = append(os.Environ(),
		"TEST_RESULT="+resultPath,
		"TEST_WORKING_DIRECTORY="+workingDirectoryPath,
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))

	result, err := os.ReadFile(resultPath)
	require.NoError(t, err)
	require.Equal(t, "trusted", strings.TrimSpace(string(result)))
	workingDirectory, err := os.ReadFile(workingDirectoryPath)
	require.NoError(t, err)
	require.Equal(t, resolvedWorktree, strings.TrimSpace(string(workingDirectory)))
}

func TestWrapperPinsScriptBackedGatesToTrustedTools(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	toolRoot := filepath.Join(root, "trusted-tools")
	worktree := filepath.Join(root, "reviewed-worktree")
	paths := []string{
		filepath.Join(toolRoot, "scripts"),
		filepath.Join(toolRoot, "tests", "antithesis"),
		filepath.Join(toolRoot, "tests", "schemathesis"),
		filepath.Join(worktree, "tests", "antithesis"),
		filepath.Join(worktree, "tests", "schemathesis"),
	}
	for _, path := range paths {
		require.NoError(t, os.MkdirAll(path, 0o755))
	}

	wrapper, err := os.ReadFile(wrapperPath(t))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(toolRoot, "scripts", "agent-just"), wrapper, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(toolRoot, "tests", "antithesis", "run_model_test.sh"), []byte(`#!/usr/bin/env bash
printf 'trusted-model\n%s\n%s\n%s\n' "$REPO" "$MODEL_HARNESS_REPO" "$*" > "$TEST_RESULT"
`), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(worktree, "tests", "antithesis", "run_model_test.sh"), []byte("printf 'target-controlled-model\\n' > \"$TEST_RESULT\"\n"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(toolRoot, "tests", "schemathesis", "run.sh"), []byte(`#!/usr/bin/env bash
printf 'trusted-schemathesis\n%s\n%s\n' "$REPO_ROOT" "$SCHEMATHESIS_OPENAPI_PATH" > "$TEST_RESULT"
`), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(worktree, "tests", "schemathesis", "run.sh"), []byte("printf 'target-controlled-schemathesis\\n' > \"$TEST_RESULT\"\n"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(worktree, "openapi.yml"), []byte("openapi: 3.0.3\n"), 0o644))
	runGit(t, worktree, "init")
	resolvedWorktree, err := filepath.EvalSymlinks(worktree)
	require.NoError(t, err)

	resultPath := filepath.Join(root, "result")
	command := exec.Command("bash", filepath.Join(toolRoot, "scripts", "agent-just"), "test-model-cluster", "180")
	command.Dir = worktree
	command.Env = append(os.Environ(), "TEST_RESULT="+resultPath)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	result, err := os.ReadFile(resultPath)
	require.NoError(t, err)
	require.Equal(t, []string{
		"trusted-model",
		resolvedWorktree,
		toolRoot,
		"--cluster 180",
	}, strings.Split(strings.TrimSpace(string(result)), "\n"))

	command = exec.Command("bash", filepath.Join(toolRoot, "scripts", "agent-just"), "test-schemathesis")
	command.Dir = worktree
	command.Env = append(os.Environ(), "TEST_RESULT="+resultPath)
	output, err = command.CombinedOutput()
	require.NoError(t, err, string(output))
	result, err = os.ReadFile(resultPath)
	require.NoError(t, err)
	require.Equal(t, []string{
		"trusted-schemathesis",
		resolvedWorktree,
		filepath.Join(resolvedWorktree, "openapi.yml"),
	}, strings.Split(strings.TrimSpace(string(result)), "\n"))
}

func wrapperPath(t *testing.T) string {
	t.Helper()

	path, err := filepath.Abs(filepath.Join("..", "agent-just"))
	require.NoError(t, err)

	return path
}

func runGit(t *testing.T, directory string, arguments ...string) {
	t.Helper()

	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, fmt.Sprintf("git %s:\n%s", strings.Join(arguments, " "), output))
}
