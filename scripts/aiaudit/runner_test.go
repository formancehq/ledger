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
)

func TestRunnerAcceptsStableUniqueFindingIDs(t *testing.T) {
	result := validResult()
	result["findings"] = []map[string]any{
		finding(testAuditID + "/restore-retries-from-snapshot"),
		finding(testAuditID + "/replay-preserves-order"),
	}

	output, err := runAudit(t, result)
	require.NoError(t, err, output)
}

func TestRunnerRejectsFindingIDFromAnotherAudit(t *testing.T) {
	result := validResult()
	result["findings"] = []map[string]any{finding("another-audit/replay-preserves-order")}

	output, err := runAudit(t, result)
	require.Error(t, err)
	require.Contains(t, output, "provider result does not match the requested audit target")
}

func TestRunnerRejectsMalformedFindingID(t *testing.T) {
	result := validResult()
	result["findings"] = []map[string]any{finding(testAuditID + "/Replay_preserves_order")}

	output, err := runAudit(t, result)
	require.Error(t, err)
	require.Contains(t, output, "provider result does not match the requested audit target")
}

func TestRunnerRejectsDuplicateFindingIDs(t *testing.T) {
	result := validResult()
	first := finding(testAuditID + "/replay-preserves-order")
	second := finding(testAuditID + "/replay-preserves-order")
	second["title"] = "Different content with the same ID"
	result["findings"] = []map[string]any{
		first,
		second,
	}

	output, err := runAudit(t, result)
	require.Error(t, err)
	require.Contains(t, output, "provider result does not match the requested audit target")
}

func runAudit(t *testing.T, result map[string]any) (string, error) {
	t.Helper()

	repositoryRoot := repositoryRoot(t)
	checkout := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(checkout, "scripts", "auditpublish"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(checkout, "docs", "technical", "audits"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(checkout, "docs", "technical", "contributing"), 0o700))
	copyFile(t, filepath.Join(repositoryRoot, "scripts", "ai-audit"), filepath.Join(checkout, "scripts", "ai-audit"))
	copyFile(t, filepath.Join(repositoryRoot, "scripts", "codex-audit.schema.json"), filepath.Join(checkout, "scripts", "codex-audit.schema.json"))
	copyFile(t, filepath.Join(repositoryRoot, "scripts", "auditpublish", "main.go"), filepath.Join(checkout, "scripts", "auditpublish", "main.go"))
	copyFile(t, filepath.Join(repositoryRoot, "go.mod"), filepath.Join(checkout, "go.mod"))
	copyFile(t, filepath.Join(repositoryRoot, "go.sum"), filepath.Join(checkout, "go.sum"))
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

	fakeBin := t.TempDir()
	fakeCodex := `#!/usr/bin/env bash
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
cp "$FAKE_CODEX_RESULT" "$output"
`
	require.NoError(t, os.WriteFile(filepath.Join(fakeBin, "codex"), []byte(fakeCodex), 0o700))

	runGit(t, checkout, "init")
	runGit(t, checkout, "config", "user.email", "audit-test@example.com")
	runGit(t, checkout, "config", "user.name", "Audit Test")
	runGit(t, checkout, "add", "--", ".")
	runGit(t, checkout, "commit", "-m", "test fixture")
	result["head"] = gitOutput(t, checkout, "rev-parse", "HEAD")
	encodedResult, err := json.Marshal(result)
	require.NoError(t, err)
	resultPath := filepath.Join(t.TempDir(), "provider-result.json")
	require.NoError(t, os.WriteFile(resultPath, encodedResult, 0o600))

	outputPath := filepath.Join(t.TempDir(), "report.json")
	command := exec.Command("bash", filepath.Join(checkout, "scripts", "ai-audit"), testAuditID, "--output", outputPath)
	command.Dir = checkout
	command.Env = testenv.Environment(
		"FAKE_CODEX_RESULT="+resultPath,
		"HOME="+t.TempDir(),
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	output, err := command.CombinedOutput()

	return string(output), err
}

func validResult() map[string]any {
	return map[string]any{
		"audit_id":        testAuditID,
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

func runGit(t *testing.T, directory string, args ...string) {
	t.Helper()
	command := testenv.Command(t, "git", args...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
}

func gitOutput(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := testenv.Command(t, "git", args...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))

	return strings.TrimSpace(string(output))
}
