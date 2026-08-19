package aiprloop

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHelpDoesNotRequirePRMetadata(t *testing.T) {
	t.Parallel()

	fakeBin := t.TempDir()
	writeExecutable(t, filepath.Join(fakeBin, "gh"), "#!/usr/bin/env bash\nexit 97\n")

	command := exec.Command("bash", launcherPath(t), "--help")
	command.Env = append(os.Environ(), "PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "Usage: bash scripts/ai-pr-loop")
}

func TestLauncherUsesUniqueWorktreesAndImmutableBase(t *testing.T) {
	t.Parallel()

	testRoot := t.TempDir()
	remote := filepath.Join(testRoot, "remote.git")
	seed := filepath.Join(testRoot, "seed")
	checkout := filepath.Join(testRoot, "checkout")

	runGit(t, testRoot, "init", "--bare", remote)
	require.NoError(t, os.MkdirAll(filepath.Join(seed, "scripts"), 0o755))
	runGit(t, seed, "init")
	runGit(t, seed, "config", "user.name", "AI PR Loop Test")
	runGit(t, seed, "config", "user.email", "ai-pr-loop@example.com")

	launcher, err := os.ReadFile(launcherPath(t))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "ai-pr-loop"), launcher, 0o755))
	writeExecutable(t, filepath.Join(seed, "scripts", "review-loop"), `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "$TEST_CAPTURE_FILE"
`)
	require.NoError(t, os.WriteFile(filepath.Join(seed, "base.txt"), []byte("base\n"), 0o644))
	runGit(t, seed, "add", ".")
	runGit(t, seed, "commit", "-m", "base")
	runGit(t, seed, "branch", "-M", "release/v3.0")
	baseSHA := runGitOutput(t, seed, "rev-parse", "HEAD")
	runGit(t, seed, "remote", "add", "origin", remote)
	runGit(t, seed, "push", "-u", "origin", "release/v3.0")

	runGit(t, seed, "checkout", "-b", "feature")
	require.NoError(t, os.WriteFile(filepath.Join(seed, "feature.txt"), []byte("feature\n"), 0o644))
	runGit(t, seed, "add", "feature.txt")
	runGit(t, seed, "commit", "-m", "feature")
	headSHA := runGitOutput(t, seed, "rev-parse", "HEAD")
	runGit(t, seed, "push", "-u", "origin", "feature")
	runGit(t, testRoot, "clone", "--branch", "release/v3.0", remote, checkout)

	fakeBin := filepath.Join(testRoot, "bin")
	require.NoError(t, os.MkdirAll(fakeBin, 0o755))
	writeExecutable(t, filepath.Join(fakeBin, "gh"), `#!/usr/bin/env bash
set -euo pipefail
case "$1 $2" in
    "repo view")
        printf 'owner/repo\n'
        ;;
    "pr view")
        printf '{"number":123,"url":"https://github.com/owner/repo/pull/123","state":"OPEN","isDraft":false,"baseRefName":"release/v3.0","baseRefOid":"%s","headRefName":"feature","headRefOid":"%s","headRepositoryOwner":{"login":"owner"},"headRepository":{"name":"repo"}}\n' "$TEST_BASE_SHA" "$TEST_HEAD_SHA"
        ;;
    *)
        exit 98
        ;;
esac
`)

	firstCapture := filepath.Join(testRoot, "first-args")
	firstOutput := runLauncher(t, checkout, fakeBin, baseSHA, headSHA, firstCapture)
	secondCapture := filepath.Join(testRoot, "second-args")
	secondOutput := runLauncher(t, checkout, fakeBin, baseSHA, headSHA, secondCapture)

	firstWorktree := worktreeFromOutput(t, firstOutput)
	secondWorktree := worktreeFromOutput(t, secondOutput)
	require.NotEqual(t, firstWorktree, secondWorktree)
	require.DirExists(t, firstWorktree)
	require.DirExists(t, secondWorktree)
	require.Equal(t, baseSHA, baseArgument(t, firstCapture))
	require.Equal(t, baseSHA, baseArgument(t, secondCapture))
}

func launcherPath(t *testing.T) string {
	t.Helper()

	path, err := filepath.Abs(filepath.Join("..", "ai-pr-loop"))
	require.NoError(t, err)

	return path
}

func runLauncher(t *testing.T, checkout, fakeBin, baseSHA, headSHA, capturePath string) string {
	t.Helper()

	command := exec.Command("bash", filepath.Join(checkout, "scripts", "ai-pr-loop"), "123", "--keep-worktree")
	command.Dir = checkout
	command.Env = append(os.Environ(),
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"TEST_BASE_SHA="+baseSHA,
		"TEST_HEAD_SHA="+headSHA,
		"TEST_CAPTURE_FILE="+capturePath,
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))

	return string(output)
}

func worktreeFromOutput(t *testing.T, output string) string {
	t.Helper()

	for line := range strings.SplitSeq(output, "\n") {
		if worktree, found := strings.CutPrefix(strings.TrimSpace(line), "worktree: "); found {
			return worktree
		}
	}
	t.Fatalf("worktree path not found in output:\n%s", output)

	return ""
}

func baseArgument(t *testing.T, capturePath string) string {
	t.Helper()

	content, err := os.ReadFile(capturePath)
	require.NoError(t, err)
	arguments := strings.Fields(string(content))
	for index, argument := range arguments {
		if argument == "--base" && index+1 < len(arguments) {
			return arguments[index+1]
		}
	}
	t.Fatalf("--base not found in captured arguments: %q", content)

	return ""
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o755))
}

func runGit(t *testing.T, directory string, arguments ...string) {
	t.Helper()
	_ = runGitOutput(t, directory, arguments...)
}

func runGitOutput(t *testing.T, directory string, arguments ...string) string {
	t.Helper()

	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, fmt.Sprintf("git %s:\n%s", strings.Join(arguments, " "), output))

	return strings.TrimSpace(string(output))
}
