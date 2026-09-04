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

	"github.com/formancehq/ledger/v3/scripts/internal/testenv"
)

const (
	testAuditID     = "persistence-restore-replay"
	firstFindingID  = testAuditID + "/restore-retries-from-snapshot"
	secondFindingID = testAuditID + "/replay-preserves-order"
	otherHead       = "0123456789abcdef0123456789abcdef01234567"
)

// The fake provider stands in for codex: it writes the prepared result to the
// destination requested by the runner and can exercise read-only boundary
// failures while the challenge is running.
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
if [[ -n "${FAKE_CODEX_PROMPT_CAPTURE:-}" ]]; then
	cat >"$FAKE_CODEX_PROMPT_CAPTURE"
fi
if [[ -n "${FAKE_CODEX_RECURSION_ATTEMPT:-}" ]] && ! grep -Fq \
	"Do not invoke scripts/ai-audit, scripts/ai-audit-challenge, scripts/ai-audit-jira" \
	"$FAKE_CODEX_PROMPT_CAPTURE"; then
	printf 'nested challenge launcher attempted\n' >"$FAKE_CODEX_RECURSION_ATTEMPT"
fi
if [[ -n "${FAKE_CODEX_DIRTY_FILE:-}" ]]; then
	printf 'mutated during challenge\n' >"$FAKE_CODEX_DIRTY_FILE"
fi
cp "$FAKE_CODEX_RESULT" "$output"
`

func TestChallengeWorkerPromptDefinesNonRecursiveLeafRole(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	output, err := fixture.run(t, fixture.validResult())
	require.NoError(t, err, output)

	prompt := readFile(t, fixture.promptCapture)
	require.Contains(t, prompt, "You are the inner challenge worker already launched by scripts/ai-audit-challenge.")
	require.Contains(t, prompt, "Do not invoke scripts/ai-audit, scripts/ai-audit-challenge, scripts/ai-audit-jira")
	require.Contains(t, prompt, "The outer trusted process owns audit and challenge orchestration, artifact publication, and Jira publication.")
	require.Less(t,
		strings.Index(prompt, "You are the inner challenge worker already launched by scripts/ai-audit-challenge."),
		strings.Index(prompt, "Read and follow:"),
	)
	require.NoFileExists(t, fixture.recursionAttempt)
}

func TestChallengeSchemaRequiresEveryTopLevelProperty(t *testing.T) {
	t.Parallel()

	repository := repositoryRoot(t)
	challengeSchema := readJSON(t, filepath.Join(repository, "scripts", "codex-audit-challenge.schema.json"))
	properties := challengeSchema["properties"].(map[string]any)
	requiredValues := challengeSchema["required"].([]any)
	required := make([]string, 0, len(requiredValues))
	for _, value := range requiredValues {
		required = append(required, value.(string))
	}

	require.ElementsMatch(t, keys(properties), required)
	require.Contains(t, required, "sourceAuditDigest")

	auditSchema := readJSON(t, filepath.Join(repository, "scripts", "codex-audit.schema.json"))
	require.NotContains(t, auditSchema["properties"].(map[string]any), "sourceAuditDigest")
}

func TestRunnerPublishesOneQualifiedResultPerOriginalFinding(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)

	output, err := fixture.run(t, fixture.validResult())
	require.NoError(t, err, output)
	require.Contains(t, output, "AI_AUDIT_CHALLENGE_RESULT: "+fixture.outputPath)
	require.Contains(t, output, "AI_AUDIT_CHALLENGE_CONFIRMED: 1")
	require.Contains(t, output, "AI_AUDIT_CHALLENGE_LIKELY: 0")
	require.Contains(t, output, "AI_AUDIT_CHALLENGE_QUESTION: 0")
	require.Contains(t, output, "AI_AUDIT_CHALLENGE_REJECTED: 1")

	published := readJSON(t, fixture.outputPath)
	require.Equal(t, testAuditID, published["audit_id"])
	require.Equal(t, fixture.head, published["head"])
	require.Equal(t, fileDigest(t, fixture.sourceReport), published["sourceAuditDigest"])
	results := published["results"].([]any)
	first := results[0].(map[string]any)
	require.Equal(t, firstFindingID, first["id"])
	require.Equal(t, "P2", first["severity"])
	require.Equal(t, "Original title of "+firstFindingID, first["title"])
	require.Equal(t, secondFindingID, results[1].(map[string]any)["id"])
}

func TestRunnerRejectsMissingSourceAuditDigest(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	result := fixture.validResult()
	delete(result, "sourceAuditDigest")

	output, err := fixture.run(t, result)
	require.Error(t, err)
	require.Contains(t, output, "result must contain a valid sourceAuditDigest")
	require.NoFileExists(t, fixture.outputPath)
}

func TestRunnerRejectsMalformedSourceAuditDigest(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	result := fixture.validResult()
	result["sourceAuditDigest"] = "sha256:not-a-digest"

	output, err := fixture.run(t, result)
	require.Error(t, err)
	require.Contains(t, output, "result must contain a valid sourceAuditDigest")
	require.NoFileExists(t, fixture.outputPath)
}

func TestRunnerRejectsSourceAuditDigestForAnotherReport(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	result := fixture.validResult()
	result["sourceAuditDigest"] = "sha256:" + strings.Repeat("0", 64)

	output, err := fixture.run(t, result)
	require.Error(t, err)
	require.Contains(t, output, "sourceAuditDigest does not match the source audit report")
	require.NoFileExists(t, fixture.outputPath)
}

func TestZeroFindingAuditCanBeChallenged(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	audit := sourceReport(fixture.head)
	audit["findings"] = []map[string]any{}

	auditOutput, sourceReport, err := fixture.runAudit(t, audit)
	require.NoError(t, err, auditOutput)
	require.Contains(t, auditOutput, "AI_AUDIT_RESULT: "+sourceReport)
	require.NoFileExists(t, fixture.recursionAttempt)

	fixture.sourceReport = sourceReport
	fixture.sourceDigest = fileDigest(t, sourceReport)
	challengeOutput, err := fixture.run(t, fixture.result())
	require.NoError(t, err, challengeOutput)
	require.Contains(t, challengeOutput, "AI_AUDIT_CHALLENGE_RESULT: "+fixture.outputPath)
	require.NoFileExists(t, fixture.recursionAttempt)

	published := readJSON(t, fixture.outputPath)
	require.Empty(t, published["results"])
	require.Equal(t, fileDigest(t, sourceReport), published["sourceAuditDigest"])
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
	result := fixture.result(challengeOutcome(firstFindingID, "CONFIRMED"))

	output, err := fixture.run(t, result)
	require.Error(t, err)
	require.Contains(t, output, "result ids do not match source findings")
	require.NoFileExists(t, fixture.outputPath)
}

func TestRunnerRejectsInventedFindingID(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	result := fixture.result(
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
	result := fixture.result(
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
	result := fixture.result(reprioritized, challengeOutcome(secondFindingID, "REJECTED"))

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
	result := fixture.result(challengeOutcome(firstFindingID, "CONFIRMED"), retitled)

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

type fixture struct {
	checkout         string
	fakeBin          string
	head             string
	promptCapture    string
	recursionAttempt string
	sourceReport     string
	sourceDigest     string
	providerResult   string
	outputPath       string
	dirtyFile        string
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
	copyFile(t, filepath.Join(repository, "scripts", "ai-audit-challenge"), filepath.Join(checkout, "scripts", "ai-audit-challenge"))
	copyFile(t, filepath.Join(repository, "scripts", "codex-audit.schema.json"), filepath.Join(checkout, "scripts", "codex-audit.schema.json"))
	copyFile(t, filepath.Join(repository, "scripts", "codex-audit-challenge.schema.json"), filepath.Join(checkout, "scripts", "codex-audit-challenge.schema.json"))
	copyFile(t, filepath.Join(repository, ".gitignore"), filepath.Join(checkout, ".gitignore"))
	require.NoError(t, os.WriteFile(
		filepath.Join(checkout, "docs", "technical", "contributing", "ai-audit.md"),
		[]byte("# Audit contract\n"),
		0o600,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(checkout, "docs", "technical", "contributing", "ai-audit-challenge.md"),
		[]byte("# Challenge contract\n"),
		0o600,
	))
	manifest := fmt.Sprintf(`{
		"id": %q,
		"title": "Test audit",
		"purpose": "Test the native audit chain",
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
	runGit(t, checkout, "config", "user.email", "audit-challenge-test@example.com")
	runGit(t, checkout, "config", "user.name", "Audit Challenge Test")
	runGit(t, checkout, "add", "--", ".")
	runGit(t, checkout, "commit", "-m", "test fixture")

	built := &fixture{
		checkout:         checkout,
		fakeBin:          fakeBin,
		head:             gitOutput(t, checkout, "rev-parse", "HEAD"),
		promptCapture:    filepath.Join(root, "challenge-prompt.txt"),
		recursionAttempt: filepath.Join(root, "challenge-recursion-attempt.txt"),
		sourceReport:     filepath.Join(root, "audit.json"),
		providerResult:   filepath.Join(root, "provider-result.json"),
	}
	physicalCheckout, err := filepath.EvalSymlinks(checkout)
	require.NoError(t, err)
	built.outputPath = filepath.Join(physicalCheckout, "build", "ai-audit", testAuditID+"-"+built.head[:12]+"-qualified.json")
	writeJSON(t, built.sourceReport, sourceReport(built.head))
	built.sourceDigest = fileDigest(t, built.sourceReport)

	return built
}

func (f *fixture) run(t *testing.T, result map[string]any) (string, error) {
	t.Helper()

	writeJSON(t, f.providerResult, result)
	command := exec.Command("bash", filepath.Join(f.checkout, "scripts", "ai-audit-challenge"), f.sourceReport)
	command.Dir = f.checkout
	environment := []string{
		"FAKE_CODEX_RESULT=" + f.providerResult,
		"FAKE_CODEX_PROMPT_CAPTURE=" + f.promptCapture,
		"FAKE_CODEX_RECURSION_ATTEMPT=" + f.recursionAttempt,
		"FAKE_CODEX_DIRTY_FILE=" + f.dirtyFile,
		"HOME=" + t.TempDir(),
		"CODEX_HOME=",
		"PATH=" + f.fakeBin + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
	command.Env = testenv.Environment(environment...)
	output, err := command.CombinedOutput()

	return string(output), err
}

func (f *fixture) runAudit(t *testing.T, result map[string]any) (string, string, error) {
	t.Helper()

	writeJSON(t, f.providerResult, result)
	command := exec.Command("bash", filepath.Join(f.checkout, "scripts", "ai-audit"), testAuditID)
	command.Dir = f.checkout
	command.Env = testenv.Environment(
		"FAKE_CODEX_RESULT="+f.providerResult,
		"FAKE_CODEX_PROMPT_CAPTURE="+f.promptCapture,
		"FAKE_CODEX_RECURSION_ATTEMPT="+f.recursionAttempt,
		"FAKE_CODEX_DIRTY_FILE=",
		"HOME="+t.TempDir(),
		"CODEX_HOME=",
		"PATH="+f.fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	output, err := command.CombinedOutput()
	sourceReport := filepath.Join(filepath.Dir(f.outputPath), testAuditID+"-"+f.head[:12]+".json")

	return string(output), sourceReport, err
}

func (f *fixture) validResult() map[string]any {
	return f.result(
		challengeOutcome(firstFindingID, "CONFIRMED"),
		challengeOutcome(secondFindingID, "REJECTED"),
	)
}

func (f *fixture) result(results ...map[string]any) map[string]any {
	result := challengeResult(f.head, results...)
	result["sourceAuditDigest"] = f.sourceDigest

	return result
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
	if results == nil {
		results = []map[string]any{}
	}

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

func fileDigest(t *testing.T, path string) string {
	t.Helper()

	content, err := os.ReadFile(path)
	require.NoError(t, err)

	return fmt.Sprintf("sha256:%x", sha256.Sum256(content))
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

func keys(values map[string]any) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}

	return result
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
	require.NoError(t, err, string(output))

	return strings.TrimSpace(string(output))
}
