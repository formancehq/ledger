package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/scripts/internal/testenv"
)

func TestClassifyFinalReview(t *testing.T) {
	t.Parallel()

	emptyContext := ""
	humanContext := "product intent is ambiguous"
	tests := []struct {
		name         string
		result       reviewResult
		wantFindings bool
		wantError    string
	}{
		{name: "approve", result: reviewResult{Decision: "APPROVE", HumanDecisionContext: &emptyContext}},
		{
			name:         "actionable findings",
			result:       reviewResult{Decision: "FINDINGS", Findings: []finding{{ID: "bug"}}, HumanDecisionContext: &emptyContext},
			wantFindings: true,
		},
		{
			name:         "human decision finding",
			result:       reviewResult{Decision: "FINDINGS", HumanDecisionContext: &humanContext},
			wantFindings: true,
		},
		{
			name:      "approve with findings",
			result:    reviewResult{Decision: "APPROVE", Findings: []finding{{ID: "bug"}}, HumanDecisionContext: &emptyContext},
			wantError: "APPROVE contains findings",
		},
		{
			name:      "empty findings result",
			result:    reviewResult{Decision: "FINDINGS", HumanDecisionContext: &emptyContext},
			wantError: "FINDINGS contains no findings",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hasFindings, err := classifyReview(test.result)
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)

				return
			}
			require.NoError(t, err)
			require.Equal(t, test.wantFindings, hasFindings)
		})
	}
}

func TestLoadFinalReviewResultContract(t *testing.T) {
	t.Parallel()

	valid := `{
		"decision":"FINDINGS",
		"head":"abc",
		"worktree_fingerprint":"fingerprint",
		"known_findings":[],
		"findings":[{
			"id":"bug","severity":"P2","blocking":true,
			"title":"bug","location":"main.go:1","evidence":"evidence",
			"impact":"impact","resolution":"resolution"
		}],
		"residual_risk":"MEDIUM",
		"human_decision_context":""
	}`
	result, err := loadReviewResult(writeReviewResult(t, valid))
	require.NoError(t, err)
	require.Equal(t, "FINDINGS", result.Decision)

	for name, replacement := range map[string]string{
		"old decision":       strings.Replace(valid, `"FINDINGS"`, `"REQUEST_CHANGES"`, 1),
		"old fixer field":    strings.Replace(valid, `"blocking":true,`, `"blocking":true,"auto_fixable":true,`, 1),
		"old previous state": strings.Replace(valid, `"known_findings":[]`, `"previous_findings":[],"known_findings":[]`, 1),
		"malformed":          `{`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := loadReviewResult(writeReviewResult(t, replacement))
			require.Error(t, err)
		})
	}
}

func TestLoadFinalReviewResultRequiresConsistentKnownFindingCoverage(t *testing.T) {
	t.Parallel()

	_, err := loadReviewResult(writeReviewResult(t, `{
		"decision":"APPROVE",
		"head":"abc",
		"worktree_fingerprint":"fingerprint",
		"known_findings":[{"id":"github-1","status":"STILL_VALID","reason":"still applies"}],
		"findings":[],
		"residual_risk":"LOW",
		"human_decision_context":""
	}`))
	require.ErrorContains(t, err, "STILL_VALID but is absent")

	_, err = loadReviewResult(writeReviewResult(t, `{
		"decision":"APPROVE",
		"head":"abc",
		"worktree_fingerprint":"fingerprint",
		"known_findings":[{"id":"github-1","status":"HUMAN_DECISION_REQUIRED","reason":"intent is absent"}],
		"findings":[],
		"residual_risk":"LOW",
		"human_decision_context":""
	}`))
	require.ErrorContains(t, err, "requires FINDINGS")
}

func TestLinearFinalReviewFlow(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "review-loop")
	build := testenv.Command(t, "go", "build", "-o", binary, ".")
	build.Dir = "."
	output, err := build.CombinedOutput()
	require.NoError(t, err, string(output))

	t.Run("straight approval", func(t *testing.T) {
		fixture := newLinearFlowFixture(t, binary)
		result := fixture.run(t, "approve", false)

		require.NoError(t, result.err, result.output)
		require.Equal(t, []string{"validation", "known-findings", "review"}, result.events)
		require.Equal(t, 1, result.validationCalls)
		require.Equal(t, 1, result.reviewerCalls)
		require.Zero(t, result.fixerCalls)
		require.Contains(t, result.output, "REVIEW_LOOP_RESULT: APPROVE")
	})

	t.Run("validation failure stops before review", func(t *testing.T) {
		fixture := newLinearFlowFixture(t, binary)
		result := fixture.run(t, "approve", true)

		require.Equal(t, exitValidationFailed, exitCode(result.err), result.output)
		require.Equal(t, []string{"validation"}, result.events)
		require.Equal(t, 1, result.validationCalls)
		require.Zero(t, result.reviewerCalls)
		require.Zero(t, result.fixerCalls)
		require.Contains(t, result.output, "REVIEW_LOOP_RESULT: VALIDATION_FAILED")
	})

	t.Run("reviewer finding stops without fixer", func(t *testing.T) {
		fixture := newLinearFlowFixture(t, binary)
		result := fixture.run(t, "findings", false)

		require.Equal(t, exitFindings, exitCode(result.err), result.output)
		require.Equal(t, 1, result.validationCalls)
		require.Equal(t, 1, result.reviewerCalls)
		require.Zero(t, result.fixerCalls)
		require.Contains(t, result.output, "REVIEW_LOOP_RESULT: FINDINGS")
		require.DirExists(t, fixture.candidate)
	})

	t.Run("modified candidate gets fresh validation and review", func(t *testing.T) {
		fixture := newLinearFlowFixture(t, binary)
		first := fixture.run(t, "approve", false)
		require.NoError(t, first.err, first.output)

		require.NoError(t, os.WriteFile(filepath.Join(fixture.candidate, "follow-up.txt"), []byte("fixed\n"), 0o644))
		runGit(t, fixture.candidate, "add", "follow-up.txt")
		runGit(t, fixture.candidate, "-c", "user.name=Review Loop Test", "-c", "user.email=review-loop@example.com", "commit", "-m", "fix finding")
		fixture.head = strings.TrimSpace(runGitOutput(t, fixture.candidate, "rev-parse", "HEAD"))
		fixture.resetCounters(t)

		second := fixture.run(t, "approve", false)
		require.NoError(t, second.err, second.output)
		require.Equal(t, []string{"validation", "known-findings", "review"}, second.events)
		require.Equal(t, 1, second.validationCalls)
		require.Equal(t, 1, second.reviewerCalls)
		require.Zero(t, second.fixerCalls)
	})

	t.Run("malformed review fails promptly without retry", func(t *testing.T) {
		fixture := newLinearFlowFixture(t, binary)
		result := fixture.run(t, "malformed", false)

		require.Equal(t, exitError, exitCode(result.err), result.output)
		require.Equal(t, 1, result.validationCalls)
		require.Equal(t, 1, result.reviewerCalls)
		require.Zero(t, result.fixerCalls)
		require.Contains(t, result.output, "REVIEW_LOOP_RESULT: REVIEW_FAILED")
	})

	t.Run("unresolved GitHub finding reaches the same final review", func(t *testing.T) {
		fixture := newLinearFlowFixture(t, binary)
		result := fixture.run(t, "known-finding", false)

		require.Equal(t, exitFindings, exitCode(result.err), result.output)
		require.Equal(t, []string{"validation", "known-findings", "review"}, result.events)
		require.Equal(t, 1, result.reviewerCalls)
		require.Contains(t, result.reviewedKnownFindings, "github-1")
	})
}

type linearFlowFixture struct {
	binary                string
	primary               string
	candidate             string
	head                  string
	validationRunDir      string
	stateDir              string
	knownFindingsFile     string
	eventsFile            string
	validationCounterFile string
	reviewerCounterFile   string
	fixerCounterFile      string
	reviewedKnownFile     string
	validator             string
	collector             string
	reviewer              string
}

type linearFlowResult struct {
	output                string
	err                   error
	events                []string
	validationCalls       int
	reviewerCalls         int
	fixerCalls            int
	reviewedKnownFindings string
}

func newLinearFlowFixture(t *testing.T, binary string) *linearFlowFixture {
	t.Helper()

	fixtureRoot := t.TempDir()
	primary := filepath.Join(fixtureRoot, "primary")
	require.NoError(t, os.MkdirAll(primary, 0o755))
	runGit(t, primary, "init")
	require.NoError(t, os.WriteFile(filepath.Join(primary, "base.txt"), []byte("base\n"), 0o644))
	runGit(t, primary, "add", "base.txt")
	runGit(t, primary, "-c", "user.name=Review Loop Test", "-c", "user.email=review-loop@example.com", "commit", "-m", "base")
	head := strings.TrimSpace(runGitOutput(t, primary, "rev-parse", "HEAD"))
	candidate := filepath.Join(fixtureRoot, "candidate")
	runGit(t, primary, "worktree", "add", "--detach", candidate, head)

	fixture := &linearFlowFixture{
		binary:                binary,
		primary:               primary,
		candidate:             candidate,
		head:                  head,
		validationRunDir:      filepath.Join(fixtureRoot, "validation"),
		stateDir:              filepath.Join(fixtureRoot, "review-state"),
		knownFindingsFile:     filepath.Join(fixtureRoot, "known-findings.json"),
		eventsFile:            filepath.Join(fixtureRoot, "events"),
		validationCounterFile: filepath.Join(fixtureRoot, "validation-count"),
		reviewerCounterFile:   filepath.Join(fixtureRoot, "reviewer-count"),
		fixerCounterFile:      filepath.Join(fixtureRoot, "fixer-count"),
		reviewedKnownFile:     filepath.Join(fixtureRoot, "reviewed-known"),
		validator:             filepath.Join(fixtureRoot, "validator.sh"),
		collector:             filepath.Join(fixtureRoot, "collector.sh"),
		reviewer:              filepath.Join(fixtureRoot, "reviewer.sh"),
	}
	require.NoError(t, os.MkdirAll(fixture.validationRunDir, 0o755))
	fixture.writeScripts(t)
	fixture.resetCounters(t)

	return fixture
}

func (fixture *linearFlowFixture) writeScripts(t *testing.T) {
	t.Helper()

	require.NoError(t, os.WriteFile(fixture.validator, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf 'validation\n' >> "$TEST_EVENTS"
printf '1\n' >> "$TEST_VALIDATION_COUNTER"
[[ "${TEST_VALIDATION_FAIL:-false}" != true ]]
`), 0o755))
	require.NoError(t, os.WriteFile(fixture.collector, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf 'known-findings\n' >> "$TEST_EVENTS"
cat > "$AI_REVIEW_KNOWN_FINDINGS" <<EOF
{"version":1,"pr_number":123,"head":"$AI_REVIEW_HEAD_FOR_FIXTURE","review_decision":"CHANGES_REQUESTED","findings":[{"id":"github-1"}]}
EOF
`), 0o755))
	require.NoError(t, os.WriteFile(fixture.reviewer, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf 'review\n' >> "$TEST_EVENTS"
printf '1\n' >> "$TEST_REVIEWER_COUNTER"
case "$TEST_REVIEW_MODE" in
  approve)
    printf '{"decision":"APPROVE","head":"%s","worktree_fingerprint":"%s","known_findings":[],"findings":[],"residual_risk":"LOW","human_decision_context":""}\n' "$AI_REVIEW_HEAD" "$AI_REVIEW_WORKTREE_FINGERPRINT" > "$AI_REVIEW_RESULT"
    ;;
  findings)
    printf '{"decision":"FINDINGS","head":"%s","worktree_fingerprint":"%s","known_findings":[],"findings":[{"id":"bug","severity":"P2","blocking":true,"title":"bug","location":"base.txt:1","evidence":"evidence","impact":"impact","resolution":"resolution"}],"residual_risk":"MEDIUM","human_decision_context":""}\n' "$AI_REVIEW_HEAD" "$AI_REVIEW_WORKTREE_FINGERPRINT" > "$AI_REVIEW_RESULT"
    ;;
  known-finding)
    cp "$AI_REVIEW_KNOWN_FINDINGS" "$TEST_REVIEWED_KNOWN"
    printf '{"decision":"FINDINGS","head":"%s","worktree_fingerprint":"%s","known_findings":[{"id":"github-1","status":"STILL_VALID","reason":"still applies"}],"findings":[{"id":"github-1","severity":"P2","blocking":true,"title":"known bug","location":"base.txt:1","evidence":"evidence","impact":"impact","resolution":"resolution"}],"residual_risk":"MEDIUM","human_decision_context":""}\n' "$AI_REVIEW_HEAD" "$AI_REVIEW_WORKTREE_FINGERPRINT" > "$AI_REVIEW_RESULT"
    ;;
  malformed) printf '{\n' > "$AI_REVIEW_RESULT" ;;
  *) exit 90 ;;
esac
`), 0o755))
}

func (fixture *linearFlowFixture) resetCounters(t *testing.T) {
	t.Helper()
	for _, path := range []string{fixture.eventsFile, fixture.validationCounterFile, fixture.reviewerCounterFile, fixture.fixerCounterFile, fixture.reviewedKnownFile} {
		require.NoError(t, os.WriteFile(path, nil, 0o600))
	}
}

func (fixture *linearFlowFixture) run(t *testing.T, mode string, validationFails bool) linearFlowResult {
	t.Helper()

	command := exec.Command(fixture.binary,
		"--base", fixture.head,
		"--pr", "123",
		"--worktree", fixture.candidate,
		"--expected-head", fixture.head,
		"--trusted-root", fixture.primary,
		"--validation-run-dir", fixture.validationRunDir,
		"--state-dir", fixture.stateDir,
		"--validation-cmd", "bash "+shellQuote(fixture.validator),
		"--known-findings-cmd", "bash "+shellQuote(fixture.collector),
		"--known-findings-file", fixture.knownFindingsFile,
		"--review-cmd", "bash "+shellQuote(fixture.reviewer),
	)
	command.Dir = fixture.candidate
	validationFlag := "false"
	if validationFails {
		validationFlag = "true"
	}
	command.Env = testenv.Environment(
		"AI_REVIEW_KNOWN_FINDINGS="+fixture.knownFindingsFile,
		"AI_REVIEW_HEAD_FOR_FIXTURE="+fixture.head,
		"TEST_EVENTS="+fixture.eventsFile,
		"TEST_VALIDATION_COUNTER="+fixture.validationCounterFile,
		"TEST_REVIEWER_COUNTER="+fixture.reviewerCounterFile,
		"TEST_FIXER_COUNTER="+fixture.fixerCounterFile,
		"TEST_REVIEWED_KNOWN="+fixture.reviewedKnownFile,
		"TEST_REVIEW_MODE="+mode,
		"TEST_VALIDATION_FAIL="+validationFlag,
	)
	output, err := command.CombinedOutput()

	return linearFlowResult{
		output:                string(output),
		err:                   err,
		events:                strings.Fields(readTestFile(t, fixture.eventsFile)),
		validationCalls:       len(strings.Fields(readTestFile(t, fixture.validationCounterFile))),
		reviewerCalls:         len(strings.Fields(readTestFile(t, fixture.reviewerCounterFile))),
		fixerCalls:            len(strings.Fields(readTestFile(t, fixture.fixerCounterFile))),
		reviewedKnownFindings: readTestFile(t, fixture.reviewedKnownFile),
	}
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) {
		return -1
	}

	return exitError.ExitCode()
}

func writeReviewResult(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "review.json")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	return path
}

func runGit(t *testing.T, directory string, arguments ...string) {
	t.Helper()
	cmd := testenv.Command(t, "git", arguments...)
	cmd.Dir = directory
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
}

func runGitOutput(t *testing.T, directory string, arguments ...string) string {
	t.Helper()
	cmd := testenv.Command(t, "git", arguments...)
	cmd.Dir = directory
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	return string(output)
}

func repositoryRootForTests(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)

	return filepath.Clean(root)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
