package aireviewknownfindings

import (
	"encoding/json"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testReviewedHead = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testLedgerHead   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testFingerprint  = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	testTrustedHead  = "dddddddddddddddddddddddddddddddddddddddd"
)

func TestNoKnownFindingsPreservesBaseResultByteForByte(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	base := newReview("APPROVE", "LOW")
	fixture.writeInputs(t, base, newLedger(), nil)
	want := readFile(t, fixture.baseResult)

	output, err := fixture.run(t, nil)
	require.NoError(t, err, output)
	require.Equal(t, want, readFile(t, fixture.result))
}

func TestStillValidKnownBlockerEscalatesApproval(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	known := newKnownFinding(101)
	base := newReview("APPROVE", "LOW")
	final := cloneObject(t, base)
	final["decision"] = "REQUEST_CHANGES"
	final["residual_risk"] = "MEDIUM"
	final["findings"] = []any{knownReviewFinding(known["id"].(string))}
	fixture.writeInputs(t, base, newLedger(known), combined(final, classification(known, "STILL_VALID")))

	output, err := fixture.run(t, nil)
	require.NoError(t, err, output)
	requireJSONEqual(t, final, readFile(t, fixture.result))
}

func TestStructuredKnownBlockerReachesReconciliation(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	known := newStructuredKnownFinding(112, 2)
	base := newReview("APPROVE", "LOW")
	final := cloneObject(t, base)
	final["decision"] = "REQUEST_CHANGES"
	final["residual_risk"] = "MEDIUM"
	final["findings"] = []any{knownReviewFinding(known["id"].(string))}
	fixture.writeInputs(t, base, newLedger(known), combined(final, classification(known, "STILL_VALID")))

	output, err := fixture.run(t, nil)
	require.NoError(t, err, output)
	requireJSONEqual(t, final, readFile(t, fixture.result))
	require.Contains(t, readFile(t, fixture.prompt), fixture.ledger)
}

func TestRejectsStructuredFindingWithoutBlockerMarker(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	known := newStructuredKnownFinding(113, 1)
	known["body"] = "unstructured prose"
	base := newReview("APPROVE", "LOW")
	fixture.writeInputs(t, base, newLedger(known), nil)

	output, err := fixture.run(t, nil)
	require.Error(t, err, output)
	require.Contains(t, output, "invalid or mismatched ledger")
}

func TestFixedKnownFindingLeavesBaseResultUnchanged(t *testing.T) {
	t.Parallel()

	assertResolvedKnownFindingLeavesBaseUnchanged(t, "FIXED")
}

func TestOutdatedKnownFindingLeavesBaseResultUnchanged(t *testing.T) {
	t.Parallel()

	assertResolvedKnownFindingLeavesBaseUnchanged(t, "OUTDATED")
}

func TestKnownHumanDecisionForcesHumanDecision(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	known := newKnownFinding(102)
	base := newReview("APPROVE", "LOW")
	final := cloneObject(t, base)
	final["decision"] = "HUMAN_DECISION_REQUIRED"
	final["residual_risk"] = "HIGH"
	final["human_decision_context"] = "The repository cannot determine the intended behavior."
	fixture.writeInputs(t, base, newLedger(known), combined(final, classification(known, "HUMAN_DECISION_REQUIRED")))

	output, err := fixture.run(t, nil)
	require.NoError(t, err, output)
	requireJSONEqual(t, final, readFile(t, fixture.result))
}

func TestRejectsRemovalOfFreshFinding(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	known := newKnownFinding(103)
	base := newReview("APPROVE", "LOW")
	base["findings"] = []any{freshFinding("fresh")}
	final := cloneObject(t, base)
	final["findings"] = []any{}
	fixture.writeInputs(t, base, newLedger(known), combined(final, classification(known, "FIXED")))

	output, err := fixture.run(t, nil)
	require.Error(t, err, output)
	require.Contains(t, output, "reconciler altered the base review")
}

func TestRejectsPreviousFindingMutation(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	known := newKnownFinding(104)
	base := newReview("APPROVE", "LOW")
	base["previous_findings"] = []any{map[string]any{
		"id": "old", "status": "FIXED", "reason": "fixed in the current state",
	}}
	final := cloneObject(t, base)
	final["previous_findings"] = []any{}
	fixture.writeInputs(t, base, newLedger(known), combined(final, classification(known, "FIXED")))

	output, err := fixture.run(t, nil)
	require.Error(t, err, output)
	require.Contains(t, output, "reconciler altered the base review")
}

func TestRejectsDecisionDowngrade(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	known := newKnownFinding(105)
	base := newReview("REQUEST_CHANGES", "MEDIUM")
	base["findings"] = []any{freshFinding("fresh-blocker")}
	final := cloneObject(t, base)
	final["decision"] = "APPROVE"
	fixture.writeInputs(t, base, newLedger(known), combined(final, classification(known, "FIXED")))

	output, err := fixture.run(t, nil)
	require.Error(t, err, output)
	require.Contains(t, output, "reconciler altered the base review")
}

func TestRejectsResidualRiskDowngrade(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	known := newKnownFinding(106)
	base := newReview("APPROVE", "HIGH")
	final := cloneObject(t, base)
	final["residual_risk"] = "LOW"
	fixture.writeInputs(t, base, newLedger(known), combined(final, classification(known, "FIXED")))

	output, err := fixture.run(t, nil)
	require.Error(t, err, output)
	require.Contains(t, output, "reconciler altered the base review")
}

func TestRejectsMissingKnownFindingClassification(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	first := newKnownFinding(107)
	second := newKnownFinding(108)
	base := newReview("APPROVE", "LOW")
	fixture.writeInputs(t, base, newLedger(first, second), combined(base, classification(first, "FIXED")))

	output, err := fixture.run(t, nil)
	require.Error(t, err, output)
	require.Contains(t, output, "incomplete known-finding reconciliation")
}

func TestRejectsLedgerHeadMismatch(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	ledger := newLedger(newKnownFinding(109))
	ledger["head"] = "dddddddddddddddddddddddddddddddddddddddd"
	base := newReview("APPROVE", "LOW")
	fixture.writeInputs(t, base, ledger, nil)

	output, err := fixture.run(t, nil)
	require.Error(t, err, output)
	require.Contains(t, output, "invalid or mismatched ledger")
}

func TestReconcilerUsesBasePinnedInstructions(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	known := newKnownFinding(111)
	base := newReview("APPROVE", "LOW")
	fixture.writeInputs(t, base, newLedger(known), combined(base, classification(known, "FIXED")))

	output, err := fixture.run(t, nil)
	require.NoError(t, err, output)
	prompt := readFile(t, fixture.prompt)
	require.Contains(t, prompt, filepath.Join(fixture.trustedRoot, "AGENTS.md"))
	require.Contains(t, prompt, filepath.Join(fixture.trustedRoot, "docs", "technical", "agent-context.md"))
	require.Contains(t, prompt, filepath.Join(fixture.trustedRoot, "docs", "technical", "contributing", "ai-review.md"))
	require.Contains(t, prompt, filepath.Join(fixture.trustedRoot, "docs", "technical", "contributing", "ai-pr-known-findings.md"))
	require.NotContains(t, prompt, filepath.Join(fixture.repository, "AGENTS.md"))
	require.NotContains(t, prompt, filepath.Join(fixture.repository, "docs", "technical", "contributing", "ai-pr-known-findings.md"))
	trustedRoot, err := filepath.EvalSymlinks(fixture.trustedRoot)
	require.NoError(t, err)
	actualCWD, err := filepath.EvalSymlinks(strings.TrimSpace(readFile(t, fixture.cwdCapture)))
	require.NoError(t, err)
	require.Equal(t, trustedRoot, actualCWD)
	require.Contains(t, prompt, fixture.repository)
}

func TestReconcilerBindsCandidateHeadSeparatelyFromTrustedToolHead(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	known := newKnownFinding(114)
	base := newReview("APPROVE", "LOW")
	fixture.writeInputs(t, base, newLedger(known), combined(base, classification(known, "FIXED")))

	output, err := fixture.run(t, nil)
	require.NoError(t, err, output)
	repository, err := filepath.EvalSymlinks(fixture.repository)
	require.NoError(t, err)
	trustedRoot, err := filepath.EvalSymlinks(fixture.trustedRoot)
	require.NoError(t, err)
	require.Equal(t, testReviewedHead, strings.TrimSpace(readFile(t, fixture.expectedHeadCapture)))
	require.Equal(t, repository, strings.TrimSpace(readFile(t, fixture.expectedWorktreeCapture)))
	require.Equal(t, testTrustedHead, strings.TrimSpace(readFile(t, fixture.trustedHeadCapture)))
	require.Equal(t, trustedRoot, strings.TrimSpace(readFile(t, fixture.trustedWorktreeCapture)))
	require.Equal(t, testTrustedHead+" "+testLedgerHead+" "+testReviewedHead, strings.TrimSpace(readFile(t, fixture.identityCapture)))
}

func assertResolvedKnownFindingLeavesBaseUnchanged(t *testing.T, status string) {
	t.Helper()

	fixture := newAdapterFixture(t)
	known := newKnownFinding(110)
	base := newReview("APPROVE", "LOW")
	fixture.writeInputs(t, base, newLedger(known), combined(base, classification(known, status)))

	output, err := fixture.run(t, nil)
	require.NoError(t, err, output)
	requireJSONEqual(t, base, readFile(t, fixture.result))
}

type adapterFixture struct {
	repository              string
	trustedRoot             string
	adapter                 string
	fakeBin                 string
	home                    string
	result                  string
	baseResult              string
	combined                string
	ledger                  string
	target                  string
	prompt                  string
	cwdCapture              string
	expectedHeadCapture     string
	expectedWorktreeCapture string
	trustedHeadCapture      string
	trustedWorktreeCapture  string
	identityCapture         string
}

func newAdapterFixture(t *testing.T) adapterFixture {
	t.Helper()

	root := t.TempDir()
	repository := filepath.Join(root, "review-target")
	trustedRoot := filepath.Join(root, "trusted-tools")
	for _, directory := range []string{repository, trustedRoot} {
		require.NoError(t, os.MkdirAll(filepath.Join(directory, "scripts"), 0o755))
		require.NoError(t, os.MkdirAll(filepath.Join(directory, "docs", "technical", "contributing"), 0o755))
	}
	runCommand(t, repository, "git", "init")
	runCommand(t, trustedRoot, "git", "init")

	copyFile(t, sourcePath(t, "ai-review-known-findings"), filepath.Join(trustedRoot, "scripts", "ai-review-known-findings"), 0o755)
	copyFile(t, sourcePath(t, "codex-review-with-known-findings.schema.json"), filepath.Join(trustedRoot, "scripts", "codex-review-with-known-findings.schema.json"), 0o644)
	writeFile(t, filepath.Join(trustedRoot, "AGENTS.md"), "# Trusted AGENTS instructions\n", 0o644)
	writeFile(t, filepath.Join(trustedRoot, "docs", "technical", "agent-context.md"), "# Trusted agent context\n", 0o644)
	writeFile(t, filepath.Join(trustedRoot, "docs", "technical", "contributing", "ai-review.md"), "# Trusted review contract\n", 0o644)
	writeFile(t, filepath.Join(trustedRoot, "docs", "technical", "contributing", "ai-pr-known-findings.md"), "# Trusted known-findings contract\n", 0o644)
	writeFile(t, filepath.Join(repository, "AGENTS.md"), "# Target-controlled AGENTS instructions\n", 0o644)
	writeFile(t, filepath.Join(repository, "docs", "technical", "agent-context.md"), "# Target-controlled agent context\n", 0o644)
	writeFile(t, filepath.Join(repository, "docs", "technical", "contributing", "ai-review.md"), "# Target-controlled review contract\n", 0o644)
	writeFile(t, filepath.Join(repository, "docs", "technical", "contributing", "ai-pr-known-findings.md"), "# Target-controlled known-findings contract\n", 0o644)
	writeFile(t, filepath.Join(trustedRoot, "scripts", "ai-review-codex"), `#!/usr/bin/env bash
set -euo pipefail
cp "$TEST_BASE_RESULT" "$AI_REVIEW_RESULT"
`, 0o755)

	fakeBin := filepath.Join(root, "bin")
	require.NoError(t, os.MkdirAll(fakeBin, 0o755))
	writeFile(t, filepath.Join(fakeBin, "codex"), `#!/usr/bin/env bash
set -euo pipefail
output=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -o) output=$2; shift 2 ;;
        *) shift ;;
    esac
done
pwd > "$TEST_CWD_CAPTURE"
printf '%s\n' "$EXPECTED_HEAD" > "$TEST_EXPECTED_HEAD_CAPTURE"
printf '%s\n' "$EXPECTED_WORKTREE" > "$TEST_EXPECTED_WORKTREE_CAPTURE"
printf '%s\n' "$AI_WORKTREE_EXPECTED_HEAD" > "$TEST_TRUSTED_HEAD_CAPTURE"
printf '%s\n' "$AI_WORKTREE_PATH" > "$TEST_TRUSTED_WORKTREE_CAPTURE"
printf '%s %s %s\n' "$TARGET_BASE_SHA" "$PR_HEAD_SHA" "$CANDIDATE_SHA" > "$TEST_IDENTITY_CAPTURE"
cat > "$TEST_PROMPT_CAPTURE"
[[ -n "$output" ]]
cp "$TEST_COMBINED_RESULT" "$output"
`, 0o755)

	home := filepath.Join(root, "home")
	require.NoError(t, os.MkdirAll(filepath.Join(home, ".codex"), 0o755))
	tmp := filepath.Join(root, "tmp")
	require.NoError(t, os.MkdirAll(tmp, 0o755))
	target := filepath.Join(root, "target.json")
	writeFile(t, target, `{"base_sha":"`+testTrustedHead+`"}`+"\n", 0o644)

	return adapterFixture{
		repository:              repository,
		trustedRoot:             trustedRoot,
		adapter:                 filepath.Join(trustedRoot, "scripts", "ai-review-known-findings"),
		fakeBin:                 fakeBin,
		home:                    home,
		result:                  filepath.Join(root, "result.json"),
		baseResult:              filepath.Join(root, "base.json"),
		combined:                filepath.Join(root, "combined.json"),
		ledger:                  filepath.Join(root, "ledger.json"),
		target:                  target,
		prompt:                  filepath.Join(root, "prompt.txt"),
		cwdCapture:              filepath.Join(root, "codex-cwd.txt"),
		expectedHeadCapture:     filepath.Join(root, "expected-head.txt"),
		expectedWorktreeCapture: filepath.Join(root, "expected-worktree.txt"),
		trustedHeadCapture:      filepath.Join(root, "trusted-head.txt"),
		trustedWorktreeCapture:  filepath.Join(root, "trusted-worktree.txt"),
		identityCapture:         filepath.Join(root, "identities.txt"),
	}
}

func (fixture adapterFixture) writeInputs(t *testing.T, base, ledger, combinedResult map[string]any) {
	t.Helper()

	writeJSON(t, fixture.baseResult, base)
	writeJSON(t, fixture.ledger, ledger)
	if combinedResult != nil {
		writeJSON(t, fixture.combined, combinedResult)
	}
}

func (fixture adapterFixture) run(t *testing.T, extra map[string]string) (string, error) {
	t.Helper()

	environment := map[string]string{
		"HOME":                           fixture.home,
		"CODEX_HOME":                     filepath.Join(fixture.home, ".codex"),
		"PATH":                           fixture.fakeBin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"TMPDIR":                         filepath.Dir(fixture.result),
		"AI_REVIEW_RESULT":               fixture.result,
		"AI_REVIEW_HEAD":                 testReviewedHead,
		"AI_REVIEW_WORKTREE_FINGERPRINT": testFingerprint,
		"AI_REVIEW_CHANGE_TARGET":        fixture.target,
		"AI_REVIEW_PASS":                 "1",
		"AI_REVIEW_KNOWN_FINDINGS":       fixture.ledger,
		"AI_REVIEW_KNOWN_FINDINGS_PR":    "123",
		"AI_REVIEW_KNOWN_FINDINGS_HEAD":  testLedgerHead,
		"TEST_BASE_RESULT":               fixture.baseResult,
		"TEST_COMBINED_RESULT":           fixture.combined,
		"TEST_PROMPT_CAPTURE":            fixture.prompt,
		"TEST_CWD_CAPTURE":               fixture.cwdCapture,
		"TEST_EXPECTED_HEAD_CAPTURE":     fixture.expectedHeadCapture,
		"TEST_EXPECTED_WORKTREE_CAPTURE": fixture.expectedWorktreeCapture,
		"TEST_TRUSTED_HEAD_CAPTURE":      fixture.trustedHeadCapture,
		"TEST_TRUSTED_WORKTREE_CAPTURE":  fixture.trustedWorktreeCapture,
		"TEST_IDENTITY_CAPTURE":          fixture.identityCapture,
	}
	maps.Copy(environment, extra)

	command := exec.Command("bash", fixture.adapter)
	command.Dir = fixture.repository
	command.Env = replaceEnvironment(os.Environ(), environment)
	output, err := command.CombinedOutput()

	return string(output), err
}

func newReview(decision, risk string) map[string]any {
	return map[string]any{
		"decision":               decision,
		"head":                   testReviewedHead,
		"worktree_fingerprint":   testFingerprint,
		"residual_risk":          risk,
		"human_decision_context": "",
		"previous_findings":      []any{},
		"findings":               []any{},
	}
}

func freshFinding(id string) map[string]any {
	return map[string]any{
		"id": id, "severity": "P2", "blocking": true, "auto_fixable": true,
		"title": "Fresh finding", "location": "scripts/example:1", "evidence": "evidence",
		"impact": "impact", "resolution": "resolution",
	}
}

func knownReviewFinding(id string) map[string]any {
	finding := freshFinding(id)
	finding["title"] = "Known finding"

	return finding
}

func newKnownFinding(id int) map[string]any {
	return map[string]any{
		"id":               "github-review-" + jsonNumber(id),
		"kind":             "review-body",
		"source_review_id": id,
		"source_id":        id,
		"url":              "https://github.com/owner/repo/pull/123#pullrequestreview-" + jsonNumber(id),
		"author":           "reviewer",
		"path":             "",
		"line":             nil,
		"body":             "Blocking review evidence",
	}
}

// newStructuredKnownFinding is a blocker-level known finding extracted from a
// structured review body rather than a whole review.
func newStructuredKnownFinding(reviewID, position int) map[string]any {
	finding := newKnownFinding(reviewID)
	finding["id"] = "github-review-" + jsonNumber(reviewID) + "-finding-" + jsonNumber(position)
	finding["kind"] = "review-body-finding"
	finding["body"] = "[P1][blocking] Structured blocking review evidence"

	return finding
}

func newLedger(findings ...map[string]any) map[string]any {
	items := make([]any, 0, len(findings))
	for _, finding := range findings {
		items = append(items, finding)
	}

	return map[string]any{
		"version": 1, "pr_number": 123, "head": testLedgerHead, "findings": items,
	}
}

func classification(finding map[string]any, status string) map[string]any {
	return map[string]any{
		"id": finding["id"], "status": status, "reason": "Verified against the current worktree",
	}
}

func combined(review map[string]any, classifications ...map[string]any) map[string]any {
	items := make([]any, 0, len(classifications))
	for _, item := range classifications {
		items = append(items, item)
	}

	return map[string]any{"review": review, "known_findings": items}
}

func cloneObject(t *testing.T, input map[string]any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(input)
	require.NoError(t, err)
	var output map[string]any
	require.NoError(t, json.Unmarshal(encoded, &output))

	return output
}

func jsonNumber(value int) string {
	return strconv.Itoa(value)
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	contents, err := json.MarshalIndent(value, "", "  ")
	require.NoError(t, err)
	writeFile(t, path, string(contents)+"\n", 0o644)
}

func requireJSONEqual(t *testing.T, expected any, actual string) {
	t.Helper()
	want, err := json.Marshal(expected)
	require.NoError(t, err)
	var got any
	require.NoError(t, json.Unmarshal([]byte(actual), &got))
	var normalized any
	require.NoError(t, json.Unmarshal(want, &normalized))
	require.Equal(t, normalized, got)
}

func sourcePath(t *testing.T, name string) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)

	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", name))
}

func copyFile(t *testing.T, source, destination string, mode os.FileMode) {
	t.Helper()
	contents, err := os.ReadFile(source)
	require.NoError(t, err)
	writeFile(t, destination, string(contents), mode)
}

func writeFile(t *testing.T, path, contents string, mode os.FileMode) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(contents), mode))
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)

	return string(contents)
}

func runCommand(t *testing.T, directory, name string, arguments ...string) string {
	if t != nil {
		t.Helper()
	}
	command := exec.Command(name, arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if t != nil {
		require.NoError(t, err, string(output))
	} else if err != nil {
		panic(string(output))
	}

	return string(output)
}

func replaceEnvironment(current []string, replacements map[string]string) []string {
	result := make([]string, 0, len(current)+len(replacements))
	for _, item := range current {
		key, _, found := strings.Cut(item, "=")
		if _, replaced := replacements[key]; found && replaced {
			continue
		}
		result = append(result, item)
	}
	for key, value := range replacements {
		result = append(result, key+"="+value)
	}

	return result
}
