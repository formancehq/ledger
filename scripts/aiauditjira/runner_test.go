package aiauditjira

import (
	"encoding/json"
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
	testAuditID   = "persistence-restore-replay"
	testFindingID = testAuditID + "/restore-retries-from-snapshot"
	testHead      = "0123456789abcdef0123456789abcdef01234567"
	createdKey    = "EN-4242"
)

// The fake acli stands in for the Atlassian CLI: it records every invocation so
// tests can assert the exact JQL the publisher builds, and answers searches and
// creations with prepared JSON.
const fakeACLI = `#!/usr/bin/env bash
set -euo pipefail
{
	printf 'invocation'
	for argument in "$@"; do printf '\t%s' "$argument"; done
	printf '\n'
} >>"$FAKE_ACLI_LOG"
if [[ "$*" == *" search "* ]]; then
	printf '%s\n' "$FAKE_ACLI_SEARCH_JSON"
else
	for ((index = 1; index <= $#; index++)); do
		if [[ "${!index}" == "--from-json" ]]; then
			next=$((index + 1))
			cp -- "${!next}" "$FAKE_ACLI_CREATE_REQUEST"
			break
		fi
	done
	printf '%s\n' "$FAKE_ACLI_CREATE_JSON"
fi
`

func TestPublisherPreviewsConfirmedFindingsWithoutContactingJira(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)

	output, err := fixture.run(t, challengeResult(confirmedResult(testFindingID)))
	require.NoError(t, err, output)
	require.Contains(t, output, "AI_AUDIT_JIRA_CANDIDATES: 1")
	require.Contains(t, output, "Marker: AI-AUDIT:"+testFindingID)
	require.Contains(t, output, "Action: DRY_RUN")
	require.NoFileExists(t, fixture.logPath)
}

// The deduplication search is the only place an untrusted finding id reaches
// Jira query syntax, so the published JQL must be the exact quoted marker
// phrase and nothing else.
func TestPublisherSearchesForTheExactQuotedMarker(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)

	output, err := fixture.run(t, challengeResult(confirmedResult(testFindingID)), "--publish")
	require.NoError(t, err, output)
	require.Contains(t, output, "Action: CREATED")
	require.Contains(t, output, "Jira: "+createdKey)
	require.Equal(t,
		`project = EN AND text ~ '"AI-AUDIT:`+testFindingID+`"'`,
		fixture.searchedJQL(t),
	)
	invocations := fixture.invocations(t)
	require.Contains(t, invocations, "\t--paginate\t")
	require.NotContains(t, invocations, "\t--limit\t")
}

func TestPublisherCreatesWithRequiredLedgerComponentInStructuredRequest(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)

	output, err := fixture.run(t, challengeResult(confirmedResult(testFindingID)), "--publish")
	require.NoError(t, err, output)
	request := fixture.createdRequest(t)
	require.Equal(t, "EN", request["projectKey"])
	require.Equal(t, "Bug", request["type"])
	require.Equal(t, "[P2] Original title of "+testFindingID, request["summary"])
	require.Equal(t, []any{"ai-audit"}, request["labels"])
	require.Equal(t, map[string]any{
		"components": []any{map[string]any{"name": "Ledger"}},
	}, request["additionalAttributes"])
	description, err := json.Marshal(request["description"])
	require.NoError(t, err)
	require.Contains(t, string(description), "AI-AUDIT:"+testFindingID)
	require.Contains(t, string(description), testHead)

	invocations := fixture.invocations(t)
	require.Contains(t, invocations, "\t--from-json\t")
	require.NotContains(t, invocations, "\t--description-file\t")
}

func TestPublisherSupportsAnExplicitJiraComponent(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)

	output, err := fixture.run(t, challengeResult(confirmedResult(testFindingID)), "--publish", "--component", "Ledger / Platform (&)")
	require.NoError(t, err, output)
	require.Equal(t, map[string]any{
		"components": []any{map[string]any{"name": "Ledger / Platform (&)"}},
	}, fixture.createdRequest(t)["additionalAttributes"])
}

func TestPublisherRejectsMalformedFindingIDsBeforeJiraCalls(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	id := testAuditID + "/broken' OR text ~ 'unrelated"
	output, err := fixture.run(t, challengeResult(confirmedResult(id)), "--publish")
	require.Error(t, err)
	require.Contains(t, output, "invalid challenge result")
	require.NoFileExists(t, fixture.logPath)
}

func TestPublisherRejectsMalformedAuditID(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	auditID := "broken' OR text ~ 'unrelated"
	result := challengeResult(confirmedResult(auditID + "/restore-retries-from-snapshot"))
	result["audit_id"] = auditID

	output, err := fixture.run(t, result, "--publish")
	require.Error(t, err)
	require.Contains(t, output, "invalid challenge result")
	require.NoFileExists(t, fixture.logPath)
}

func TestPublisherRejectsDuplicateFindingIDsBeforeAnyJiraCall(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	result := challengeResult(
		confirmedResult(testFindingID),
		confirmedResult(testFindingID),
	)

	output, err := fixture.run(t, result, "--publish")
	require.Error(t, err)
	require.Contains(t, output, "invalid challenge result")
	require.NoFileExists(t, fixture.logPath)
}

// An issue is only a duplicate if it actually carries the marker, so the search
// must request the field the publisher writes the marker into.
func TestPublisherReportsExistingIssueCarryingTheExactMarker(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	fixture.searchJSON = searchResponse(t, "EN-77", "Confirmed Ledger deep-audit finding.\n\nAI-AUDIT:"+testFindingID+"\n")

	output, err := fixture.run(t, challengeResult(confirmedResult(testFindingID)), "--publish")
	require.NoError(t, err, output)
	require.Contains(t, output, "Action: EXISTS")
	require.Contains(t, output, "Jira: EN-77")
	require.NotContains(t, output, "Action: CREATED")
	require.Contains(t, fixture.searchedArgument(t, "--fields"), "description")
}

// Jira text search is word-based, so it can return issues that do not carry
// this marker. Treating such a result as a duplicate would silently drop the
// confirmed finding, so publication must fall through to creation whenever the
// exact marker is absent from the description the publisher writes it into.
func TestPublisherCreatesWhenSearchResultsLackTheExactMarker(t *testing.T) {
	t.Parallel()

	marker := "AI-AUDIT:" + testFindingID
	for name, searchJSON := range map[string]string{
		"issue without any marker":           searchResponse(t, "EN-77", "Unrelated issue mentioning the audit"),
		"marker of another finding":          searchResponse(t, "EN-78", "AI-AUDIT:"+testAuditID+"/another-confirmed-finding"),
		"longer marker sharing this prefix":  searchResponse(t, "EN-79", marker+"-extended"),
		"issue returned without description": `{"issues":[{"key":"EN-80","fields":{"summary":"[P2] Some Jira summary"}}]}`,
		"marker only in the summary":         `{"issues":[{"key":"EN-81","fields":{"summary":"` + marker + `","description":"No marker here"}}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fixture := newFixture(t)
			fixture.searchJSON = searchJSON

			output, err := fixture.run(t, challengeResult(confirmedResult(testFindingID)), "--publish")
			require.NoError(t, err, output)
			require.Contains(t, output, "Action: CREATED")
			require.Contains(t, output, "Jira: "+createdKey)
			require.NotContains(t, output, "Action: EXISTS")
		})
	}
}

func searchResponse(t *testing.T, key string, description string) string {
	t.Helper()

	encoded, err := json.Marshal(map[string]any{
		"issues": []map[string]any{{
			"key": key,
			"fields": map[string]any{
				"summary":     "[P2] Some Jira summary",
				"description": description,
			},
		}},
	})
	require.NoError(t, err)

	return string(encoded)
}

type fixture struct {
	inputPath     string
	logPath       string
	createRequest string
	fakeBin       string
	searchJSON    string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	root := t.TempDir()
	fakeBin := filepath.Join(root, "bin")
	require.NoError(t, os.MkdirAll(fakeBin, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(fakeBin, "acli"), []byte(fakeACLI), 0o700))

	return &fixture{
		inputPath:     filepath.Join(root, "qualified.json"),
		logPath:       filepath.Join(root, "acli-invocations.log"),
		createRequest: filepath.Join(root, "acli-create-request.json"),
		fakeBin:       fakeBin,
		searchJSON:    `{"issues":[]}`,
	}
}

func (f *fixture) run(t *testing.T, result map[string]any, arguments ...string) (string, error) {
	t.Helper()

	writeJSON(t, f.inputPath, result)
	command := exec.Command("bash", append(
		[]string{filepath.Join(repositoryRoot(t), "scripts", "ai-audit-jira"), f.inputPath},
		arguments...,
	)...)
	command.Env = testenv.Environment(
		"FAKE_ACLI_LOG="+f.logPath,
		"FAKE_ACLI_SEARCH_JSON="+f.searchJSON,
		`FAKE_ACLI_CREATE_JSON={"key":"`+createdKey+`"}`,
		"FAKE_ACLI_CREATE_REQUEST="+f.createRequest,
		"PATH="+f.fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	output, err := command.CombinedOutput()

	return string(output), err
}

func (f *fixture) searchedJQL(t *testing.T) string {
	t.Helper()

	return f.searchedArgument(t, "--jql")
}

func (f *fixture) searchedArgument(t *testing.T, name string) string {
	t.Helper()

	contents, err := os.ReadFile(f.logPath)
	require.NoError(t, err)
	for invocation := range strings.SplitSeq(strings.TrimSuffix(string(contents), "\n"), "\n") {
		arguments := strings.Split(invocation, "\t")
		for index, argument := range arguments {
			if argument == name {
				require.Less(t, index+1, len(arguments))

				return arguments[index+1]
			}
		}
	}
	t.Fatalf("no acli invocation carried a %s argument: %s", name, contents)

	return ""
}

func (f *fixture) invocations(t *testing.T) string {
	t.Helper()

	contents, err := os.ReadFile(f.logPath)
	require.NoError(t, err)

	return string(contents)
}

func (f *fixture) createdRequest(t *testing.T) map[string]any {
	t.Helper()

	contents, err := os.ReadFile(f.createRequest)
	require.NoError(t, err)
	request := map[string]any{}
	require.NoError(t, json.Unmarshal(contents, &request))

	return request
}

func challengeResult(results ...map[string]any) map[string]any {
	return map[string]any{
		"audit_id":      testAuditID,
		"head":          testHead,
		"summary":       "Test challenge",
		"results":       results,
		"questions":     []map[string]any{},
		"residual_risk": "LOW",
	}
}

func confirmedResult(id string) map[string]any {
	return map[string]any{
		"id":                      id,
		"severity":                "P2",
		"title":                   "Original title of " + id,
		"status":                  "CONFIRMED",
		"challenge_summary":       "Reconstructed the claimed failure path",
		"evidence_for":            []string{"scripts/ai-audit-jira"},
		"evidence_against":        []string{},
		"invariant_assessment":    "The invariant is documented",
		"reachability_assessment": "The state is reachable",
		"existing_tests":          []string{},
		"reproduction_plan":       "Run the publisher fixture",
		"recommended_next_action": "Implement a regression test",
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)

	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}

func writeJSON(t *testing.T, path string, content map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(content)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, encoded, 0o600))
}
