package aiauditchallenge

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testAuditID     = "persistence-restore-replay"
	firstFindingID  = testAuditID + "/restore-retries-from-snapshot"
	secondFindingID = testAuditID + "/replay-preserves-order"
	otherHead       = "0123456789abcdef0123456789abcdef01234567"
)

// The fake provider stands in for codex: it writes the prepared result to the
// destination requested by the runner and, when asked, mutates the challenged
// repository or replaces the caller-owned source audit report while the
// challenge is still running.
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
	printf 'mutated during challenge\n' >"$FAKE_CODEX_DIRTY_FILE"
fi
if [[ -n "${FAKE_CODEX_REPLACEMENT_REPORT:-}" ]]; then
	cp "$FAKE_CODEX_REPLACEMENT_REPORT" "$FAKE_CODEX_SOURCE_REPORT"
fi
cp "$FAKE_CODEX_RESULT" "$output"
`

func TestRunnerPublishesOneQualifiedResultPerOriginalFinding(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)

	output, err := fixture.run(t, fixture.validResult())
	require.NoError(t, err, output)
	require.Contains(t, output, "AI_AUDIT_CHALLENGE_RESULT: "+resolvePath(t, fixture.outputPath))
	require.Contains(t, output, "AI_AUDIT_CHALLENGE_CONFIRMED: 1")
	require.Contains(t, output, "AI_AUDIT_CHALLENGE_LIKELY: 0")
	require.Contains(t, output, "AI_AUDIT_CHALLENGE_QUESTION: 0")
	require.Contains(t, output, "AI_AUDIT_CHALLENGE_REJECTED: 1")

	published := readJSON(t, fixture.outputPath)
	require.Equal(t, testAuditID, published["audit_id"])
	require.Equal(t, fixture.head, published["head"])
	require.Equal(t, fileDigest(t, fixture.sourceReport), published["sourceAuditDigest"])
	require.Equal(t, []string{firstFindingID, secondFindingID}, resultIDs(t, published))
}

func TestRunnerRejectsResultForAnotherAudit(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	result := fixture.validResult()
	result["audit_id"] = "another-audit"

	output, err := fixture.run(t, result)
	require.Error(t, err)
	require.Contains(t, output, "result target mismatch")
	require.NoFileExists(t, fixture.outputPath)
}

func TestRunnerRejectsResultForAnotherHead(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	result := fixture.validResult()
	result["head"] = otherHead

	output, err := fixture.run(t, result)
	require.Error(t, err)
	require.Contains(t, output, "result target mismatch")
	require.NoFileExists(t, fixture.outputPath)
}

func TestRunnerRejectsOmittedFinding(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	result := challengeResult(fixture.head, challengeOutcome(firstFindingID, "CONFIRMED"))

	output, err := fixture.run(t, result)
	require.Error(t, err)
	require.Contains(t, output, "result ids do not match source findings")
	require.NoFileExists(t, fixture.outputPath)
}

func TestRunnerRejectsInventedFindingID(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	result := challengeResult(fixture.head,
		challengeOutcome(firstFindingID, "CONFIRMED"),
		challengeOutcome(secondFindingID, "REJECTED"),
		challengeOutcome(testAuditID+"/invented-during-challenge", "CONFIRMED"),
	)

	output, err := fixture.run(t, result)
	require.Error(t, err)
	require.Contains(t, output, "result ids do not match source findings")
	require.NoFileExists(t, fixture.outputPath)
}

// A duplicated id can never reproduce the source id multiset, so it is caught
// by the one-to-one id comparison rather than by the uniqueness assertion that
// backs it up.
func TestRunnerRejectsDuplicateFindingID(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	result := challengeResult(fixture.head,
		challengeOutcome(firstFindingID, "CONFIRMED"),
		challengeOutcome(firstFindingID, "REJECTED"),
	)

	output, err := fixture.run(t, result)
	require.Error(t, err)
	require.Contains(t, output, "result ids do not match source findings")
	require.NoFileExists(t, fixture.outputPath)
}

func TestRunnerRejectsReprioritizedFinding(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	reprioritized := challengeOutcome(firstFindingID, "CONFIRMED")
	reprioritized["severity"] = "P3"
	result := challengeResult(fixture.head, reprioritized, challengeOutcome(secondFindingID, "REJECTED"))

	output, err := fixture.run(t, result)
	require.Error(t, err)
	require.Contains(t, output, "must preserve the original severity and title")
	require.NoFileExists(t, fixture.outputPath)
}

func TestRunnerRejectsRetitledFinding(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	retitled := challengeOutcome(secondFindingID, "REJECTED")
	retitled["title"] = "A different subject for the same finding"
	result := challengeResult(fixture.head, challengeOutcome(firstFindingID, "CONFIRMED"), retitled)

	output, err := fixture.run(t, result)
	require.Error(t, err)
	require.Contains(t, output, "must preserve the original severity and title")
	require.NoFileExists(t, fixture.outputPath)
}

func TestRunnerRejectsDirtyWorktree(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	require.NoError(t, os.WriteFile(filepath.Join(fixture.checkout, "untracked.txt"), []byte("dirty\n"), 0o600))

	output, err := fixture.run(t, fixture.validResult())
	require.Error(t, err)
	require.Contains(t, output, "repository worktree must be clean")
	require.NoFileExists(t, fixture.outputPath)
}

func TestRunnerRejectsHeadThatDoesNotMatchTheAuditedCommit(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	require.NoError(t, os.WriteFile(filepath.Join(fixture.checkout, "later.txt"), []byte("later\n"), 0o600))
	runGit(t, fixture.checkout, "add", "--", "later.txt")
	runGit(t, fixture.checkout, "commit", "-m", "advance head")

	output, err := fixture.run(t, fixture.validResult())
	require.Error(t, err)
	require.Contains(t, output, "does not match audited HEAD "+fixture.head)
	require.NoFileExists(t, fixture.outputPath)
}

func TestRunnerRejectsResultWhenTheRepositoryChangesDuringTheChallenge(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	fixture.dirtyFile = filepath.Join(fixture.checkout, "mutated-during-challenge.txt")

	output, err := fixture.run(t, fixture.validResult())
	require.Error(t, err)
	require.Contains(t, output, "repository state changed during challenge")
	require.NoFileExists(t, fixture.outputPath)
}

// The source report belongs to the caller and may be edited or replaced while
// the challenge runs. Qualification is bound to the snapshot taken before
// validation, so replacing the external report mid-run can neither retarget the
// published qualification nor make the id/metadata comparisons pass against a
// different audit.
func TestRunnerStaysBoundToTheSnapshotWhenTheSourceReportIsReplacedDuringTheChallenge(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	replacement := map[string]any{
		"audit_id":        "replaced-during-challenge",
		"head":            otherHead,
		"summary":         "Replaced audit",
		"inspected_areas": []string{"scripts"},
		"findings":        []map[string]any{sourceFinding("replaced-during-challenge/other-finding")},
		"questions":       []map[string]any{},
		"residual_risk":   "LOW",
	}
	fixture.replacementReport = filepath.Join(t.TempDir(), "replacement-audit.json")
	writeJSON(t, fixture.replacementReport, replacement)

	output, err := fixture.run(t, fixture.validResult())
	require.NoError(t, err, output)
	require.Equal(t, "replaced-during-challenge", readJSON(t, fixture.sourceReport)["audit_id"])

	published := readJSON(t, fixture.outputPath)
	require.Equal(t, testAuditID, published["audit_id"])
	require.Equal(t, fixture.head, published["head"])
	require.Equal(t, []string{firstFindingID, secondFindingID}, resultIDs(t, published))
}

func TestRunnerRejectsSymlinkOutputDestination(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	protected := filepath.Join(t.TempDir(), "protected.json")
	require.NoError(t, os.WriteFile(protected, []byte("protected\n"), 0o600))
	fixture.outputPath = filepath.Join(filepath.Dir(fixture.sourceReport), "redirected.json")
	require.NoError(t, os.Symlink(protected, fixture.outputPath))

	output, err := fixture.run(t, fixture.validResult())
	require.Error(t, err)
	require.Contains(t, output, "--output must not be a symlink")
	require.Equal(t, "protected\n", readFile(t, protected))
}

func TestRunnerRejectsOutputDestinationInTrackedRepositoryContent(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	fixture.outputPath = filepath.Join(fixture.checkout, "scripts", "codex-audit-challenge.schema.json")
	tracked := readFile(t, fixture.outputPath)

	output, err := fixture.run(t, fixture.validResult())
	require.Error(t, err)
	require.Contains(t, output, "must not overwrite tracked repository content")
	require.Equal(t, tracked, readFile(t, fixture.outputPath))
}

func TestRunnerRejectsOutputDestinationOverwritingTheSourceReport(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	fixture.outputPath = fixture.sourceReport
	original := readFile(t, fixture.sourceReport)

	output, err := fixture.run(t, fixture.validResult())
	require.Error(t, err)
	require.Contains(t, output, "must not overwrite the source audit report")
	require.Equal(t, original, readFile(t, fixture.sourceReport))
}

func TestRunnerRejectsUnignoredRepositoryOutput(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	fixture.outputPath = filepath.Join(fixture.checkout, "report.json")

	output, err := fixture.run(t, fixture.validResult())
	require.Error(t, err)
	require.Contains(t, output, "repository-local --output must be under build/")
	require.NoFileExists(t, fixture.outputPath)
}

func TestRunnerRejectsRepositoryOutputBeforeCreatingDirectories(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	createdDirectory := filepath.Join(fixture.checkout, "internal", "challenge-created")
	fixture.outputPath = filepath.Join(createdDirectory, "report.json")

	output, err := fixture.run(t, fixture.validResult())
	require.Error(t, err)
	require.Contains(t, output, "repository-local --output must be under build/")
	require.NoDirExists(t, createdDirectory)
}

func TestRunnerRejectsSymlinkedRepositoryOutputBeforeCreatingDirectories(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	repositoryLink := filepath.Join(t.TempDir(), "checkout-link")
	require.NoError(t, os.Symlink(fixture.checkout, repositoryLink))
	createdDirectory := filepath.Join(fixture.checkout, "internal", "challenge-created")
	fixture.outputPath = filepath.Join(repositoryLink, "internal", "challenge-created", "report.json")

	output, err := fixture.run(t, fixture.validResult())
	require.Error(t, err)
	require.Contains(t, output, "repository-local --output must be under build/")
	require.NoDirExists(t, createdDirectory)
}

func TestRunnerRejectsGitMetadataOutputBeforeCreatingDirectories(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	createdDirectory := filepath.Join(fixture.checkout, ".git", "challenge-created")
	fixture.outputPath = filepath.Join(createdDirectory, "report.json")

	output, err := fixture.run(t, fixture.validResult())
	require.Error(t, err)
	require.Contains(t, output, "must not write inside Git metadata")
	require.NoDirExists(t, createdDirectory)
}

func TestRunnerAllowsIgnoredRepositoryOutputUnderBuild(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	fixture.outputPath = filepath.Join(fixture.checkout, "build", "ai-audit", "qualified.json")

	output, err := fixture.run(t, fixture.validResult())
	require.NoError(t, err, output)
	require.FileExists(t, fixture.outputPath)
	require.Empty(t, gitOutput(t, fixture.checkout, "status", "--porcelain", "--untracked-files=normal"))
}

type fixture struct {
	checkout       string
	fakeBin        string
	head           string
	sourceReport   string
	providerResult string
	outputPath     string
	// dirtyFile, when set, is the path the fake provider writes inside the
	// challenged checkout to simulate a mutation during the challenge.
	dirtyFile string
	// replacementReport, when set, is the audit report the fake provider copies
	// over the caller-owned source report while the challenge is running.
	replacementReport string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	root := t.TempDir()
	checkout := filepath.Join(root, "checkout")
	repository := repositoryRoot(t)
	require.NoError(t, os.MkdirAll(filepath.Join(checkout, "scripts", "auditpublish"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(checkout, "docs", "technical", "contributing"), 0o700))
	copyFile(t, filepath.Join(repository, "scripts", "ai-audit-challenge"), filepath.Join(checkout, "scripts", "ai-audit-challenge"))
	copyFile(t, filepath.Join(repository, "scripts", "codex-audit-challenge.schema.json"), filepath.Join(checkout, "scripts", "codex-audit-challenge.schema.json"))
	copyFile(t, filepath.Join(repository, "scripts", "auditpublish", "main.go"), filepath.Join(checkout, "scripts", "auditpublish", "main.go"))
	copyFile(t, filepath.Join(repository, "go.mod"), filepath.Join(checkout, "go.mod"))
	copyFile(t, filepath.Join(repository, "go.sum"), filepath.Join(checkout, "go.sum"))
	copyFile(t, filepath.Join(repository, ".gitignore"), filepath.Join(checkout, ".gitignore"))
	require.NoError(t, os.WriteFile(
		filepath.Join(checkout, "docs", "technical", "contributing", "ai-audit-challenge.md"),
		[]byte("# Challenge contract\n"),
		0o600,
	))

	fakeBin := filepath.Join(root, "bin")
	require.NoError(t, os.MkdirAll(fakeBin, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(fakeBin, "codex"), []byte(fakeCodex), 0o700))

	runGit(t, checkout, "init")
	runGit(t, checkout, "config", "user.email", "audit-challenge-test@example.com")
	runGit(t, checkout, "config", "user.name", "Audit Challenge Test")
	runGit(t, checkout, "add", "--", ".")
	runGit(t, checkout, "commit", "-m", "test fixture")

	built := &fixture{
		checkout:       checkout,
		fakeBin:        fakeBin,
		head:           gitOutput(t, checkout, "rev-parse", "HEAD"),
		sourceReport:   filepath.Join(root, "audit.json"),
		providerResult: filepath.Join(root, "provider-result.json"),
		outputPath:     filepath.Join(root, "qualified.json"),
	}
	writeJSON(t, built.sourceReport, sourceReport(built.head))

	return built
}

func (f *fixture) run(t *testing.T, result map[string]any) (string, error) {
	t.Helper()

	writeJSON(t, f.providerResult, result)
	command := exec.Command("bash", filepath.Join(f.checkout, "scripts", "ai-audit-challenge"), f.sourceReport, "--output", f.outputPath)
	command.Dir = f.checkout
	command.Env = append(os.Environ(),
		"FAKE_CODEX_RESULT="+f.providerResult,
		"FAKE_CODEX_DIRTY_FILE="+f.dirtyFile,
		"FAKE_CODEX_SOURCE_REPORT="+f.sourceReport,
		"FAKE_CODEX_REPLACEMENT_REPORT="+f.replacementReport,
		"HOME="+t.TempDir(),
		"CODEX_HOME=",
		"PATH="+f.fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	command.Env = append(command.Env, goCaches(t)...)
	output, err := command.CombinedOutput()

	return string(output), err
}

func (f *fixture) validResult() map[string]any {
	return challengeResult(f.head,
		challengeOutcome(firstFindingID, "CONFIRMED"),
		challengeOutcome(secondFindingID, "REJECTED"),
	)
}

func sourceReport(head string) map[string]any {
	return map[string]any{
		"audit_id":        testAuditID,
		"head":            head,
		"summary":         "Test audit",
		"inspected_areas": []string{"scripts"},
		"findings":        []map[string]any{sourceFinding(firstFindingID), sourceFinding(secondFindingID)},
		"questions":       []map[string]any{},
		"residual_risk":   "LOW",
	}
}

// The runner only reads the id, severity and title of an original finding; the
// rest of the audit payload is irrelevant to the challenge contract.
func sourceFinding(id string) map[string]any {
	return map[string]any{
		"id":       id,
		"severity": "P2",
		"title":    "Original title of " + id,
	}
}

func challengeResult(head string, results ...map[string]any) map[string]any {
	return map[string]any{
		"audit_id":      testAuditID,
		"head":          head,
		"summary":       "Test challenge",
		"results":       results,
		"questions":     []map[string]any{},
		"residual_risk": "LOW",
	}
}

func challengeOutcome(id, status string) map[string]any {
	return map[string]any{
		"id":                      id,
		"severity":                "P2",
		"title":                   "Original title of " + id,
		"status":                  status,
		"challenge_summary":       "Reconstructed the claimed failure path",
		"evidence_for":            []string{"scripts/ai-audit-challenge"},
		"evidence_against":        []string{},
		"invariant_assessment":    "The invariant is documented",
		"reachability_assessment": "The state is reachable",
		"existing_tests":          []string{},
		"reproduction_plan":       "Run the challenge fixture",
		"recommended_next_action": "Implement a regression test",
	}
}

func resultIDs(t *testing.T, report map[string]any) []string {
	t.Helper()
	results, ok := report["results"].([]any)
	require.True(t, ok)
	ids := make([]string, 0, len(results))
	for _, entry := range results {
		result, ok := entry.(map[string]any)
		require.True(t, ok)
		id, ok := result["id"].(string)
		require.True(t, ok)
		ids = append(ids, id)
	}

	return ids
}

func fileDigest(t *testing.T, path string) string {
	t.Helper()

	content, err := os.ReadFile(path)
	require.NoError(t, err)

	return fmt.Sprintf("sha256:%x", sha256.Sum256(content))
}

// The runner builds the trusted publisher from the fixture checkout with an
// isolated HOME, so Go's caches must be passed explicitly; otherwise every test
// rebuilds the standard library and may try to resolve modules from the
// network.
func goCaches(t *testing.T) []string {
	t.Helper()
	command := exec.Command("go", "env", "GOCACHE", "GOMODCACHE")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	values := strings.Split(strings.TrimSpace(string(output)), "\n")
	require.Len(t, values, 2)

	return []string{
		"GOCACHE=" + strings.TrimSpace(values[0]),
		"GOMODCACHE=" + strings.TrimSpace(values[1]),
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)

	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}

// The runner reports and publishes to the physically resolved destination, so
// assertions on its output must compare against the resolved path.
func resolvePath(t *testing.T, path string) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(filepath.Dir(path))
	require.NoError(t, err)

	return filepath.Join(directory, filepath.Base(path))
}

func copyFile(t *testing.T, source, destination string) {
	t.Helper()
	contents, err := os.ReadFile(source)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(destination, contents, 0o600))
}

func writeJSON(t *testing.T, path string, content map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(content)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, encoded, 0o600))
}

func readJSON(t *testing.T, path string) map[string]any {
	t.Helper()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	decoded := map[string]any{}
	require.NoError(t, json.Unmarshal(contents, &decoded))

	return decoded
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)

	return string(contents)
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
	require.NoError(t, err, string(output))

	return strings.TrimSpace(string(output))
}
