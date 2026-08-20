package aiprtriage

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunnerExecutesCodexAtExactPRHead(t *testing.T) {
	fixture := newFixture(t, "repo")

	output, err := fixture.run(t)
	require.NoError(t, err, output)
	require.Equal(t, fixture.headSHA, strings.TrimSpace(readFile(t, fixture.codexHeadCapture)))
	require.Equal(t, "false", strings.TrimSpace(readFile(t, fixture.callerMarkerCapture)))

	codexDirectory := strings.TrimSpace(readFile(t, fixture.codexDirectoryCapture))
	require.NoDirExists(t, codexDirectory)
	require.FileExists(t, fixture.resultPath)
}

func TestRunnerRejectsDifferentRepositoryWithSameOwner(t *testing.T) {
	fixture := newFixture(t, "different-repo")

	output, err := fixture.run(t)
	require.Error(t, err)
	require.Contains(t, output, "cross-repository PRs are not supported yet")
	require.NoFileExists(t, fixture.codexHeadCapture)
}

type fixture struct {
	checkout              string
	fakeBin               string
	baseSHA               string
	headSHA               string
	headRepository        string
	resultPath            string
	codexHeadCapture      string
	codexDirectoryCapture string
	callerMarkerCapture   string
}

func newFixture(t *testing.T, headRepository string) fixture {
	t.Helper()

	testRoot := t.TempDir()
	remote := filepath.Join(testRoot, "remote.git")
	seed := filepath.Join(testRoot, "seed")
	checkout := filepath.Join(testRoot, "checkout")
	require.NoError(t, os.MkdirAll(filepath.Join(seed, "scripts"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(seed, "docs", "technical", "contributing"), 0o755))

	runGit(t, testRoot, "init", "--bare", remote)
	runGit(t, seed, "init")
	runGit(t, seed, "config", "user.name", "AI PR Triage Test")
	runGit(t, seed, "config", "user.email", "ai-pr-triage@example.com")
	copyFile(t, runnerPath(t), filepath.Join(seed, "scripts", "ai-pr-triage"), 0o755)
	copyFile(t, schemaPath(t), filepath.Join(seed, "scripts", "codex-pr-triage.schema.json"), 0o644)
	require.NoError(t, os.WriteFile(
		filepath.Join(seed, "docs", "technical", "contributing", "ai-pr-triage.md"),
		[]byte("# AI PR triage contract\n"),
		0o644,
	))
	require.NoError(t, os.WriteFile(filepath.Join(seed, "base.txt"), []byte("base\n"), 0o644))
	runGit(t, seed, "add", "--", ".")
	runGit(t, seed, "commit", "-m", "base")
	runGit(t, seed, "branch", "-M", "release/v3.0")
	baseSHA := gitOutput(t, seed, "rev-parse", "HEAD")
	runGit(t, seed, "remote", "add", "origin", remote)
	runGit(t, seed, "push", "-u", "origin", "release/v3.0")

	runGit(t, seed, "checkout", "-b", "feature")
	require.NoError(t, os.WriteFile(filepath.Join(seed, "head.txt"), []byte("head\n"), 0o644))
	runGit(t, seed, "add", "--", "head.txt")
	runGit(t, seed, "commit", "-m", "feature")
	headSHA := gitOutput(t, seed, "rev-parse", "HEAD")
	runGit(t, seed, "push", "-u", "origin", "feature")
	runGit(t, testRoot, "clone", "--branch", "release/v3.0", remote, checkout)
	require.NoError(t, os.WriteFile(filepath.Join(checkout, "caller-only.txt"), []byte("caller\n"), 0o644))

	fakeBin := filepath.Join(testRoot, "bin")
	require.NoError(t, os.MkdirAll(fakeBin, 0o755))
	writeExecutable(t, filepath.Join(fakeBin, "gh"), `#!/usr/bin/env bash
set -euo pipefail
case "$1 $2" in
  "repo view")
    printf 'owner/repo\n'
    ;;
  "pr view")
    printf '{"number":123,"state":"OPEN","baseRefName":"release/v3.0","baseRefOid":"%s","headRefName":"feature","headRefOid":"%s","headRepositoryOwner":{"login":"owner"},"headRepository":{"name":"%s"},"title":"test","body":"test","url":"https://github.com/owner/repo/pull/123"}\n' "$TEST_BASE_SHA" "$TEST_HEAD_SHA" "$TEST_HEAD_REPOSITORY"
    ;;
  *)
    exit 98
    ;;
esac
`)
	writeExecutable(t, filepath.Join(fakeBin, "codex"), `#!/usr/bin/env bash
set -euo pipefail
output=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "-o" ]]; then
    output=$2
    shift 2
    continue
  fi
  shift
done
cat >/dev/null
pwd >"$TEST_CODEX_DIRECTORY_CAPTURE"
git rev-parse HEAD >"$TEST_CODEX_HEAD_CAPTURE"
if [[ -e caller-only.txt ]]; then printf 'true\n'; else printf 'false\n'; fi >"$TEST_CALLER_MARKER_CAPTURE"
printf '{"decision":"KEEP","base_sha":"%s","head":"%s","problem_statement":"test","documented_needs":[],"technical_decisions":[],"existing_alternatives":[],"cost_assessment":"test","consequence_of_doing_nothing":"test","questions_for_author":[],"summary":"test"}\n' "$TEST_BASE_SHA" "$TEST_HEAD_SHA" >"$output"
`)

	return fixture{
		checkout:              checkout,
		fakeBin:               fakeBin,
		baseSHA:               baseSHA,
		headSHA:               headSHA,
		headRepository:        headRepository,
		resultPath:            filepath.Join(testRoot, "result.json"),
		codexHeadCapture:      filepath.Join(testRoot, "codex-head"),
		codexDirectoryCapture: filepath.Join(testRoot, "codex-directory"),
		callerMarkerCapture:   filepath.Join(testRoot, "caller-marker"),
	}
}

func (f fixture) run(t *testing.T) (string, error) {
	t.Helper()

	command := exec.Command("bash", filepath.Join(f.checkout, "scripts", "ai-pr-triage"), "123", "--output", f.resultPath)
	command.Dir = f.checkout
	command.Env = append(os.Environ(),
		"PATH="+f.fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"HOME="+t.TempDir(),
		"TEST_BASE_SHA="+f.baseSHA,
		"TEST_HEAD_SHA="+f.headSHA,
		"TEST_HEAD_REPOSITORY="+f.headRepository,
		"TEST_CODEX_HEAD_CAPTURE="+f.codexHeadCapture,
		"TEST_CODEX_DIRECTORY_CAPTURE="+f.codexDirectoryCapture,
		"TEST_CALLER_MARKER_CAPTURE="+f.callerMarkerCapture,
	)

	output, err := command.CombinedOutput()

	return string(output), err
}

func runnerPath(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)

	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "ai-pr-triage"))
}

func schemaPath(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)

	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "codex-pr-triage.schema.json"))
}

func copyFile(t *testing.T, source, destination string, mode os.FileMode) {
	t.Helper()
	contents, err := os.ReadFile(source)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(destination, contents, mode))
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o755))
}

func runGit(t *testing.T, directory string, arguments ...string) {
	t.Helper()
	_ = gitOutput(t, directory, arguments...)
}

func gitOutput(t *testing.T, directory string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, fmt.Sprintf("git %s:\n%s", strings.Join(arguments, " "), output))

	return strings.TrimSpace(string(output))
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)

	return string(contents)
}
