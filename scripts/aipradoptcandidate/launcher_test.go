package aipradoptcandidate

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdoptCandidateUsesTheSameMechanicalWorktreeBinding(t *testing.T) {
	t.Parallel()

	fixture := newAdoptionFixture(t)
	statusBefore := runGitOutput(t, fixture.checkout, "status", "--porcelain=v1", "--untracked-files=all")
	command := exec.Command("bash", filepath.Join(fixture.checkout, "scripts", "ai-pr-adopt-candidate"), "123", fixture.candidateSHA)
	command.Dir = fixture.checkout
	command.Env = append(os.Environ(),
		"PATH="+fixture.fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"TEST_BASE_SHA="+fixture.baseSHA,
		"TEST_HEAD_SHA="+fixture.headSHA,
		"TEST_CAPTURE="+fixture.capture,
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "AI_PR_ADOPT_RESULT: APPROVED_NOT_PUSHED")

	arguments := strings.Split(strings.TrimSpace(readFile(t, fixture.capture)), "\n")
	worktree := argumentValue(t, arguments, "--worktree")
	require.Equal(t, worktree, strings.TrimSpace(readFile(t, fixture.capture+".cwd")))
	require.Equal(t, "123", argumentValue(t, arguments, "--pr"))
	require.Equal(t, fixture.candidateSHA, argumentValue(t, arguments, "--expected-head"))
	require.NotEqual(t, fixture.checkout, worktree)
	resolvedCheckout, resolveErr := filepath.EvalSymlinks(fixture.checkout)
	require.NoError(t, resolveErr)
	require.Equal(t, resolvedCheckout, argumentValue(t, arguments, "--trusted-root"))
	require.NotEqual(t, worktree, argumentValue(t, arguments, "--validation-run-dir"))
	require.Equal(t, statusBefore, runGitOutput(t, fixture.checkout, "status", "--porcelain=v1", "--untracked-files=all"))
}

type adoptionFixture struct {
	root         string
	remote       string
	checkout     string
	fakeBin      string
	baseSHA      string
	headSHA      string
	candidateSHA string
	capture      string
}

func newAdoptionFixture(t *testing.T) adoptionFixture {
	t.Helper()

	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	seed := filepath.Join(root, "seed")
	checkout := filepath.Join(root, "checkout")
	require.NoError(t, os.MkdirAll(filepath.Join(seed, "scripts"), 0o755))
	runGit(t, root, "init", "--bare", remote)
	runGit(t, seed, "init")
	runGit(t, seed, "config", "user.name", "Adoption Test")
	runGit(t, seed, "config", "user.email", "adoption@example.com")

	copyFile(t, adoptionScriptPath(t), filepath.Join(seed, "scripts", "ai-pr-adopt-candidate"), 0o755)
	copyFile(t, filepath.Join(filepath.Dir(adoptionScriptPath(t)), "ai-git-guard"), filepath.Join(seed, "scripts", "ai-git-guard"), 0o755)
	writeExecutable(t, filepath.Join(seed, "scripts", "ai-review-codex"), "#!/usr/bin/env bash\nexit 0\n")
	writeExecutable(t, filepath.Join(seed, "scripts", "agent-check-pr"), "#!/usr/bin/env bash\nexit 0\n")
	writeExecutable(t, filepath.Join(seed, "scripts", "agent-just"), "#!/usr/bin/env bash\nexit 0\n")
	writeExecutable(t, filepath.Join(seed, "scripts", "review-loop"), `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "$TEST_CAPTURE"
pwd > "$TEST_CAPTURE.cwd"
`)
	require.NoError(t, os.WriteFile(filepath.Join(seed, "base.txt"), []byte("base\n"), 0o644))
	runGit(t, seed, "add", ".")
	runGit(t, seed, "commit", "-m", "base")
	runGit(t, seed, "branch", "-M", "release/v3.0")
	baseSHA := strings.TrimSpace(runGitOutput(t, seed, "rev-parse", "HEAD"))
	runGit(t, seed, "remote", "add", "origin", remote)
	runGit(t, seed, "push", "-u", "origin", "release/v3.0")

	runGit(t, seed, "switch", "-c", "feature")
	require.NoError(t, os.WriteFile(filepath.Join(seed, "feature.txt"), []byte("feature\n"), 0o644))
	runGit(t, seed, "add", "feature.txt")
	runGit(t, seed, "commit", "-m", "feature")
	headSHA := strings.TrimSpace(runGitOutput(t, seed, "rev-parse", "HEAD"))
	runGit(t, seed, "push", "-u", "origin", "feature")

	runGit(t, root, "clone", "--branch", "release/v3.0", remote, checkout)
	runGit(t, checkout, "config", "user.name", "Adoption Test")
	runGit(t, checkout, "config", "user.email", "adoption@example.com")
	runGit(t, checkout, "fetch", "origin", "feature")
	runGit(t, checkout, "switch", "-c", "candidate", "origin/feature")
	require.NoError(t, os.WriteFile(filepath.Join(checkout, "fix.txt"), []byte("fix\n"), 0o644))
	runGit(t, checkout, "add", "fix.txt")
	runGit(t, checkout, "commit", "-m", "candidate")
	candidateSHA := strings.TrimSpace(runGitOutput(t, checkout, "rev-parse", "HEAD"))
	runGit(t, checkout, "switch", "release/v3.0")

	fakeBin := filepath.Join(root, "bin")
	require.NoError(t, os.MkdirAll(fakeBin, 0o755))
	writeExecutable(t, filepath.Join(fakeBin, "gh"), `#!/usr/bin/env bash
set -euo pipefail
case "$1 $2" in
    "repo view") printf 'owner/repo\n' ;;
    "pr view")
        printf '{"number":123,"url":"https://github.com/owner/repo/pull/123","state":"OPEN","baseRefName":"release/v3.0","baseRefOid":"%s","headRefName":"feature","headRefOid":"%s","headRepositoryOwner":{"login":"owner"},"headRepository":{"name":"repo"}}\n' "$TEST_BASE_SHA" "$TEST_HEAD_SHA"
        ;;
    *) exit 98 ;;
esac
`)
	writeExecutable(t, filepath.Join(fakeBin, "nix"), `#!/usr/bin/env bash
set -euo pipefail
all_arguments=" $* "
if [[ "$all_arguments" != *" build "* ]]; then
    while [[ $# -gt 0 && "$1" != "--command" ]]; do shift; done
    [[ $# -gt 0 ]]
    shift
    exec "$@"
fi
source_root=""
output=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -C) source_root=$2; shift 2 ;;
        -o) output=$2; shift 2 ;;
        *) shift ;;
    esac
done
[[ -n "$source_root" && -n "$output" ]]
cp "$source_root/scripts/review-loop" "$output"
chmod 755 "$output"
`)

	return adoptionFixture{
		root:         root,
		remote:       remote,
		checkout:     checkout,
		fakeBin:      fakeBin,
		baseSHA:      baseSHA,
		headSHA:      headSHA,
		candidateSHA: candidateSHA,
		capture:      filepath.Join(root, "review-args"),
	}
}

func adoptionScriptPath(t *testing.T) string {
	t.Helper()

	path, err := filepath.Abs(filepath.Join("..", "ai-pr-adopt-candidate"))
	require.NoError(t, err)

	return path
}

func copyFile(t *testing.T, source, destination string, mode os.FileMode) {
	t.Helper()

	content, err := os.ReadFile(source)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(destination, content, mode))
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

	return string(output)
}

func readFile(t *testing.T, path string) string {
	t.Helper()

	content, err := os.ReadFile(path)
	require.NoError(t, err)

	return string(content)
}

func argumentValue(t *testing.T, arguments []string, name string) string {
	t.Helper()

	for index, argument := range arguments {
		if argument == name && index+1 < len(arguments) {
			return arguments[index+1]
		}
	}
	require.FailNow(t, "missing argument", name)

	return ""
}
