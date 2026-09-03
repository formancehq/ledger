package aiaudit

import (
	"encoding/json"
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

const (
	testAuditID = "persistence-restore-replay"
	otherHead   = "0123456789abcdef0123456789abcdef01234567"
)

const fakeCodex = `#!/usr/bin/env bash
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
if [[ -n "${FAKE_CODEX_DIRTY_FILE:-}" ]]; then
	printf 'mutated during audit\n' >"$FAKE_CODEX_DIRTY_FILE"
fi
if [[ -n "${FAKE_CODEX_FAIL:-}" ]]; then
	exit 42
fi
cp "$FAKE_CODEX_RESULT" "$output"
`

func TestRunnerRejectsDirtyWorktree(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	require.NoError(t, os.WriteFile(filepath.Join(fixture.checkout, "dirty.txt"), []byte("dirty\n"), 0o600))

	output, err := fixture.run(t, fixture.validResult())
	require.Error(t, err)
	require.Contains(t, output, "repository worktree must be clean")
	require.NoFileExists(t, fixture.outputPath)
}

func TestRunnerRejectsWrongHead(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	result := fixture.validResult()
	result["head"] = otherHead

	output, err := fixture.run(t, result)
	require.Error(t, err)
	require.Contains(t, output, "provider result does not match the requested audit target")
	require.NoFileExists(t, fixture.outputPath)
}

func TestRunnerRejectsMalformedProviderOutput(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	output, err := fixture.runRaw(t, []byte("not json\n"))
	require.Error(t, err)
	require.Contains(t, output, "provider result does not match the requested audit target")
	require.NoFileExists(t, fixture.outputPath)
}

func TestRunnerRejectsDuplicateFindingIDs(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	result := fixture.validResult()
	first := finding(testAuditID + "/replay-preserves-order")
	second := finding(testAuditID + "/replay-preserves-order")
	second["title"] = "Different content with the same ID"
	result["findings"] = []map[string]any{first, second}

	output, err := fixture.run(t, result)
	require.Error(t, err)
	require.Contains(t, output, "provider result does not match the requested audit target")
	require.NoFileExists(t, fixture.outputPath)
}

func TestRunnerRejectsRepositoryMutationDuringAudit(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	fixture.dirtyFile = filepath.Join(fixture.checkout, "mutated-during-audit.txt")

	output, err := fixture.run(t, fixture.validResult())
	require.Error(t, err)
	require.Contains(t, output, "repository state changed during audit")
	require.NoFileExists(t, fixture.outputPath)
}

func TestRunnerPublishesStableUniqueFindingsAtomicallyAfterValidation(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	require.NoError(t, os.MkdirAll(filepath.Dir(fixture.outputPath), 0o700))
	require.NoError(t, os.WriteFile(fixture.outputPath, []byte("previous result\n"), 0o600))

	output, err := fixture.runRaw(t, []byte("not json\n"))
	require.Error(t, err)
	require.Contains(t, output, "provider result does not match the requested audit target")
	require.Equal(t, "previous result\n", readFile(t, fixture.outputPath))
	require.Empty(t, temporaryResults(t, fixture))

	result := fixture.validResult()
	result["findings"] = []map[string]any{
		finding(testAuditID + "/restore-retries-from-snapshot"),
		finding(testAuditID + "/replay-preserves-order"),
	}
	output, err = fixture.run(t, result)
	require.NoError(t, err, output)
	require.Contains(t, output, "AI_AUDIT_RESULT: "+fixture.outputPath)
	requireJSONEqual(t, result, fixture.outputPath)
}

func TestRunnerProviderFailureLeavesNoFinalArtifact(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	fixture.providerFails = true

	_, err := fixture.run(t, fixture.validResult())
	require.Error(t, err)
	require.NoFileExists(t, fixture.outputPath)
	require.Empty(t, temporaryResults(t, fixture))
}

type fixture struct {
	checkout      string
	fakeBin       string
	head          string
	providerPath  string
	outputPath    string
	dirtyFile     string
	providerFails bool
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	root := t.TempDir()
	checkout := filepath.Join(root, "checkout")
	repository := repositoryRoot(t)
	require.NoError(t, os.MkdirAll(filepath.Join(checkout, "scripts"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(checkout, "docs", "technical", "audits"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(checkout, "docs", "technical", "contributing"), 0o700))
	copyFile(t, filepath.Join(repository, "scripts", "ai-audit"), filepath.Join(checkout, "scripts", "ai-audit"))
	copyFile(t, filepath.Join(repository, "scripts", "codex-audit.schema.json"), filepath.Join(checkout, "scripts", "codex-audit.schema.json"))
	copyFile(t, filepath.Join(repository, ".gitignore"), filepath.Join(checkout, ".gitignore"))
	require.NoError(t, os.WriteFile(
		filepath.Join(checkout, "docs", "technical", "contributing", "ai-audit.md"),
		[]byte("# Audit contract\n"),
		0o600,
	))
	manifest := fmt.Sprintf(`{
		"id": %q,
		"title": "Test audit",
		"purpose": "Test the runner",
		"paths": ["scripts/**"],
		"related_docs": [],
		"invariants": ["Results are validated"],
		"adversarial_questions": [],
		"dynamic_checks_to_consider": []
	}`, testAuditID)
	require.NoError(t, os.WriteFile(
		filepath.Join(checkout, "docs", "technical", "audits", testAuditID+".json"),
		[]byte(manifest),
		0o600,
	))

	fakeBin := filepath.Join(root, "bin")
	require.NoError(t, os.MkdirAll(fakeBin, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(fakeBin, "codex"), []byte(fakeCodex), 0o700))

	runGit(t, checkout, "init")
	runGit(t, checkout, "config", "user.email", "audit-test@example.com")
	runGit(t, checkout, "config", "user.name", "Audit Test")
	runGit(t, checkout, "add", "--", ".")
	runGit(t, checkout, "commit", "-m", "test fixture")
	head := gitOutput(t, checkout, "rev-parse", "HEAD")
	physicalCheckout, err := filepath.EvalSymlinks(checkout)
	require.NoError(t, err)

	return &fixture{
		checkout:     checkout,
		fakeBin:      fakeBin,
		head:         head,
		providerPath: filepath.Join(root, "provider-result.json"),
		outputPath:   filepath.Join(physicalCheckout, "build", "ai-audit", testAuditID+"-"+head[:12]+".json"),
	}
}

func (f *fixture) run(t *testing.T, result map[string]any) (string, error) {
	t.Helper()

	encoded, err := json.Marshal(result)
	require.NoError(t, err)

	return f.runRaw(t, encoded)
}

func (f *fixture) runRaw(t *testing.T, result []byte) (string, error) {
	t.Helper()

	require.NoError(t, os.WriteFile(f.providerPath, result, 0o600))
	providerFail := ""
	if f.providerFails {
		providerFail = "1"
	}
	command := exec.Command("bash", filepath.Join(f.checkout, "scripts", "ai-audit"), testAuditID)
	command.Dir = f.checkout
	command.Env = testenv.Environment(
		"FAKE_CODEX_RESULT="+f.providerPath,
		"FAKE_CODEX_DIRTY_FILE="+f.dirtyFile,
		"FAKE_CODEX_FAIL="+providerFail,
		"HOME="+t.TempDir(),
		"CODEX_HOME=",
		"PATH="+f.fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	output, err := command.CombinedOutput()

	return string(output), err
}

func (f *fixture) validResult() map[string]any {
	return map[string]any{
		"audit_id":        testAuditID,
		"head":            f.head,
		"summary":         "Test result",
		"inspected_areas": []string{"scripts"},
		"findings":        []map[string]any{},
		"questions":       []map[string]any{},
		"residual_risk":   "LOW",
	}
}

func finding(id string) map[string]any {
	return map[string]any{
		"id":                 id,
		"severity":           "P2",
		"title":              "Test finding",
		"location":           "scripts/ai-audit",
		"violated_invariant": "Results are validated",
		"failure_path":       "The provider returns invalid data",
		"impact":             "The report cannot be correlated",
		"evidence":           "The fixture contains an invalid ID",
		"reproduction_plan":  "Run the audit fixture",
		"test_gap":           "No prior runner test",
		"confidence":         "HIGH",
	}
}

func temporaryResults(t *testing.T, f *fixture) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(f.outputPath), "."+testAuditID+"-*"))
	require.NoError(t, err)

	return matches
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)

	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}

func copyFile(t *testing.T, source, destination string) {
	t.Helper()
	contents, err := os.ReadFile(source)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(destination, contents, 0o600))
}

func requireJSONEqual(t *testing.T, expected map[string]any, path string) {
	t.Helper()
	expectedJSON, err := json.Marshal(expected)
	require.NoError(t, err)
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.JSONEq(t, string(expectedJSON), string(contents))
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)

	return string(contents)
}

func runGit(t *testing.T, directory string, args ...string) {
	t.Helper()
	_ = gitOutput(t, directory, args...)
}

func gitOutput(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := testenv.Command(t, "git", args...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))

	return strings.TrimSpace(string(output))
}
