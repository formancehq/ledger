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

	"github.com/formancehq/ledger/v3/scripts/internal/testenv"
)

func TestRunnerPinsPolicyToBaseAndExposesExactPRHeadAsEvidence(t *testing.T) {
	fixture := newFixture(t, "repo")

	output, err := fixture.run(t)
	require.NoError(t, err, output)
	require.Equal(t, fixture.baseSHA, strings.TrimSpace(readFile(t, fixture.codexHeadCapture)))
	require.Equal(t, fixture.headSHA, strings.TrimSpace(readFile(t, fixture.evidenceHeadCapture)))
	require.Equal(t, fixture.headSHA, strings.TrimSpace(readFile(t, fixture.expectedHeadCapture)))
	require.Equal(t, fixture.baseSHA, strings.TrimSpace(readFile(t, fixture.trustedHeadCapture)))
	require.Equal(t, fixture.baseSHA+" "+fixture.headSHA+" ", strings.TrimSuffix(readFile(t, fixture.identityCapture), "\n"))
	require.Equal(t, "trusted", strings.TrimSpace(readFile(t, fixture.trustedInstructionsCapture)))
	require.Equal(t, "false", strings.TrimSpace(readFile(t, fixture.callerMarkerCapture)))
	require.Contains(t, readFile(t, fixture.promptCapture), "product-technical-traceability.md")

	codexDirectory := strings.TrimSpace(readFile(t, fixture.codexDirectoryCapture))
	require.NoDirExists(t, codexDirectory)
	require.FileExists(t, fixture.resultPath)
}

func TestRunnerDiffsFromMergeBaseWhenBaseAdvances(t *testing.T) {
	fixture := newFixture(t, "repo")

	output, err := fixture.run(t)
	require.NoError(t, err, output)
	prompt := readFile(t, fixture.promptCapture)
	require.Contains(t, prompt, "- base_sha: "+fixture.baseSHA)
	require.Contains(t, prompt, "- merge_base: "+fixture.mergeBaseSHA)
	require.Contains(t, prompt, "diff --find-renames "+fixture.mergeBaseSHA+" "+fixture.headSHA+" --")
	require.NotContains(t, prompt, "diff --find-renames "+fixture.baseSHA+" "+fixture.headSHA+" --")
}

func TestRunnerRejectsDifferentRepositoryWithSameOwner(t *testing.T) {
	fixture := newFixture(t, "different-repo")

	output, err := fixture.run(t)
	require.Error(t, err)
	require.Contains(t, output, "cross-repository PRs are not supported yet")
	require.NoFileExists(t, fixture.codexHeadCapture)
}

func TestRunnerDetectsDirtyRootContentMutationWithUnchangedStatus(t *testing.T) {
	fixture := newFixture(t, "repo")
	fixture.rootMutation = "dirty-content"

	output, err := fixture.run(t)
	require.Error(t, err, output)
	require.Contains(t, output, "ROOT_MUTATION_DETECTED")
	require.Equal(t, "different caller content\n", readFile(t, filepath.Join(fixture.checkout, "caller-only.txt")))
}

type fixture struct {
	checkout                   string
	fakeBin                    string
	baseSHA                    string
	mergeBaseSHA               string
	headSHA                    string
	headRepository             string
	resultPath                 string
	codexHeadCapture           string
	evidenceHeadCapture        string
	codexDirectoryCapture      string
	callerMarkerCapture        string
	promptCapture              string
	trustedInstructionsCapture string
	expectedHeadCapture        string
	trustedHeadCapture         string
	identityCapture            string
	rootMutation               string
}

func newFixture(t *testing.T, headRepository string) fixture {
	t.Helper()

	testRoot := t.TempDir()
	remote := filepath.Join(testRoot, "remote.git")
	seed := filepath.Join(testRoot, "seed")
	checkout := filepath.Join(testRoot, "checkout")
	require.NoError(t, os.MkdirAll(filepath.Join(seed, "scripts", "rootguard"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(seed, "scripts", "internal", "rootguard"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(seed, "docs", "technical", "contributing"), 0o755))

	runGit(t, testRoot, "init", "--bare", remote)
	runGit(t, seed, "init")
	runGit(t, seed, "config", "user.name", "AI PR Triage Test")
	runGit(t, seed, "config", "user.email", "ai-pr-triage@example.com")
	copyFile(t, runnerPath(t), filepath.Join(seed, "scripts", "ai-pr-triage"), 0o755)
	copyFile(t, schemaPath(t), filepath.Join(seed, "scripts", "codex-pr-triage.schema.json"), 0o644)
	copyFile(t, rootguardMainPath(t), filepath.Join(seed, "scripts", "rootguard", "main.go"), 0o644)
	copyFile(t, rootguardPackagePath(t), filepath.Join(seed, "scripts", "internal", "rootguard", "rootguard.go"), 0o644)
	require.NoError(t, os.WriteFile(
		filepath.Join(seed, "go.mod"),
		[]byte("module github.com/formancehq/ledger/v3\n\ngo 1.26.0\n"),
		0o644,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(seed, "docs", "technical", "contributing", "ai-pr-triage.md"),
		[]byte("# AI PR triage contract\n"),
		0o644,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(seed, "docs", "technical", "contributing", "product-technical-traceability.md"),
		[]byte("# Product to technical traceability\n"),
		0o644,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(seed, "docs", "technical", "agent-context.md"),
		[]byte("# Trusted agent context\n"),
		0o644,
	))
	require.NoError(t, os.WriteFile(filepath.Join(seed, "AGENTS.md"), []byte("trusted base instruction\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(seed, "base.txt"), []byte("base\n"), 0o644))
	runGit(t, seed, "add", "--", ".")
	runGit(t, seed, "commit", "-m", "base")
	runGit(t, seed, "branch", "-M", "release/v3.0")
	mergeBaseSHA := gitOutput(t, seed, "rev-parse", "HEAD")
	runGit(t, seed, "remote", "add", "origin", remote)
	runGit(t, seed, "push", "-u", "origin", "release/v3.0")

	runGit(t, seed, "checkout", "-b", "feature")
	require.NoError(t, os.WriteFile(filepath.Join(seed, "AGENTS.md"), []byte("untrusted head instruction\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(seed, "head.txt"), []byte("head\n"), 0o644))
	require.NoError(t, os.WriteFile(
		filepath.Join(seed, "scripts", "rootguard", "main.go"),
		[]byte("package main\n\nfunc main() {}\n"),
		0o644,
	))
	runGit(t, seed, "add", "--", "AGENTS.md", "head.txt", "scripts/rootguard/main.go")
	runGit(t, seed, "commit", "-m", "feature")
	headSHA := gitOutput(t, seed, "rev-parse", "HEAD")
	runGit(t, seed, "push", "-u", "origin", "feature")

	runGit(t, seed, "checkout", "release/v3.0")
	require.NoError(t, os.WriteFile(filepath.Join(seed, "base-tip.txt"), []byte("advanced base\n"), 0o644))
	runGit(t, seed, "add", "--", "base-tip.txt")
	runGit(t, seed, "commit", "-m", "advance base")
	baseSHA := gitOutput(t, seed, "rev-parse", "HEAD")
	runGit(t, seed, "push", "origin", "release/v3.0")
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
cat >"$TEST_PROMPT_CAPTURE"
pwd >"$TEST_CODEX_DIRECTORY_CAPTURE"
git rev-parse HEAD >"$TEST_CODEX_HEAD_CAPTURE"
git -C ../head-worktree rev-parse HEAD >"$TEST_EVIDENCE_HEAD_CAPTURE"
printf '%s\n' "$EXPECTED_HEAD" >"$TEST_EXPECTED_HEAD_CAPTURE"
printf '%s\n' "$AI_WORKTREE_EXPECTED_HEAD" >"$TEST_TRUSTED_HEAD_CAPTURE"
printf '%s %s %s\n' "$TARGET_BASE_SHA" "$PR_HEAD_SHA" "${CANDIDATE_SHA:-}" >"$TEST_IDENTITY_CAPTURE"
if grep -qx 'trusted base instruction' AGENTS.md && grep -qx 'untrusted head instruction' ../head-worktree/AGENTS.md; then
  printf 'trusted\n' >"$TEST_TRUSTED_INSTRUCTIONS_CAPTURE"
else
  printf 'untrusted\n' >"$TEST_TRUSTED_INSTRUCTIONS_CAPTURE"
fi
if [[ -e caller-only.txt ]]; then printf 'true\n'; else printf 'false\n'; fi >"$TEST_CALLER_MARKER_CAPTURE"
if [[ "${TEST_ROOT_MUTATION:-}" == dirty-content ]]; then
  printf 'different caller content\n' >"$TEST_CALLER_CHECKOUT/caller-only.txt"
fi
printf '{"decision":"KEEP","base_sha":"%s","head":"%s","problem_statement":"test","documented_needs":[],"technical_decisions":[],"existing_alternatives":[],"cost_assessment":"test","consequence_of_doing_nothing":"test","questions_for_author":[],"summary":"test"}\n' "$TEST_BASE_SHA" "$TEST_HEAD_SHA" >"$output"
`)
	writeExecutable(t, filepath.Join(fakeBin, "nix"), `#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == develop && "$3" == --command ]]
shift 3
exec "$@"
`)

	return fixture{
		checkout:                   checkout,
		fakeBin:                    fakeBin,
		baseSHA:                    baseSHA,
		mergeBaseSHA:               mergeBaseSHA,
		headSHA:                    headSHA,
		headRepository:             headRepository,
		resultPath:                 filepath.Join(testRoot, "result.json"),
		codexHeadCapture:           filepath.Join(testRoot, "codex-head"),
		evidenceHeadCapture:        filepath.Join(testRoot, "evidence-head"),
		codexDirectoryCapture:      filepath.Join(testRoot, "codex-directory"),
		callerMarkerCapture:        filepath.Join(testRoot, "caller-marker"),
		promptCapture:              filepath.Join(testRoot, "prompt"),
		trustedInstructionsCapture: filepath.Join(testRoot, "trusted-instructions"),
		expectedHeadCapture:        filepath.Join(testRoot, "expected-head"),
		trustedHeadCapture:         filepath.Join(testRoot, "trusted-head"),
		identityCapture:            filepath.Join(testRoot, "identities"),
	}
}

func (f fixture) run(t *testing.T) (string, error) {
	t.Helper()

	command := exec.Command("bash", filepath.Join(f.checkout, "scripts", "ai-pr-triage"), "123", "--output", f.resultPath)
	command.Dir = f.checkout
	command.Env = testenv.Environment(
		"PATH="+f.fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"HOME="+t.TempDir(),
		"TEST_BASE_SHA="+f.baseSHA,
		"TEST_HEAD_SHA="+f.headSHA,
		"TEST_HEAD_REPOSITORY="+f.headRepository,
		"TEST_CODEX_HEAD_CAPTURE="+f.codexHeadCapture,
		"TEST_EVIDENCE_HEAD_CAPTURE="+f.evidenceHeadCapture,
		"TEST_CODEX_DIRECTORY_CAPTURE="+f.codexDirectoryCapture,
		"TEST_CALLER_MARKER_CAPTURE="+f.callerMarkerCapture,
		"TEST_PROMPT_CAPTURE="+f.promptCapture,
		"TEST_TRUSTED_INSTRUCTIONS_CAPTURE="+f.trustedInstructionsCapture,
		"TEST_EXPECTED_HEAD_CAPTURE="+f.expectedHeadCapture,
		"TEST_TRUSTED_HEAD_CAPTURE="+f.trustedHeadCapture,
		"TEST_IDENTITY_CAPTURE="+f.identityCapture,
		"TEST_CALLER_CHECKOUT="+f.checkout,
		"TEST_ROOT_MUTATION="+f.rootMutation,
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

func rootguardMainPath(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)

	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "rootguard", "main.go"))
}

func rootguardPackagePath(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)

	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "internal", "rootguard", "rootguard.go"))
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
	command := testenv.Command(t, "git", arguments...)
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
