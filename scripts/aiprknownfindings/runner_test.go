package aiprknownfindings

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
	testHead      = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testMovedHead = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

type ledger struct {
	PRNumber       int    `json:"pr_number"`
	Head           string `json:"head"`
	ReviewDecision string `json:"review_decision"`
	Findings       []struct {
		ID           string `json:"id"`
		Kind         string `json:"kind"`
		Path         string `json:"path"`
		Line         *int   `json:"line"`
		OriginalLine *int   `json:"original_line"`
		IsOutdated   bool   `json:"is_outdated"`
		Body         string `json:"body"`
	} `json:"findings"`
}

func TestCollectorReturnsEmptySetWithoutUnresolvedFindings(t *testing.T) {
	t.Parallel()

	result := runCollector(t, "empty", false)
	require.Empty(t, result.Findings)
}

func TestCollectorIncludesOneCurrentUnresolvedThread(t *testing.T) {
	t.Parallel()

	result := runCollector(t, "current", false)
	require.Len(t, result.Findings, 1)
	require.Equal(t, "github-review-thread-THREAD_1", result.Findings[0].ID)
	require.Equal(t, "unresolved-review-thread", result.Findings[0].Kind)
	require.Equal(t, "scripts/example", result.Findings[0].Path)
	require.Equal(t, 7, *result.Findings[0].Line)
	require.Equal(t, 7, *result.Findings[0].OriginalLine)
	require.False(t, result.Findings[0].IsOutdated)
	require.Contains(t, result.Findings[0].Body, "Untrusted review evidence")
}

func TestCollectorExcludesResolvedThread(t *testing.T) {
	t.Parallel()

	result := runCollector(t, "resolved", false)
	require.Empty(t, result.Findings)
}

func TestCollectorIncludesEveryUnresolvedThread(t *testing.T) {
	t.Parallel()

	result := runCollector(t, "multiple", false)
	require.Len(t, result.Findings, 2)
	require.Equal(t, "github-review-thread-THREAD_1", result.Findings[0].ID)
	require.Equal(t, "github-review-thread-THREAD_2", result.Findings[1].ID)
}

func TestCollectorRepresentsOutdatedThreadAccurately(t *testing.T) {
	t.Parallel()

	result := runCollector(t, "outdated", false)
	require.Len(t, result.Findings, 1)
	require.True(t, result.Findings[0].IsOutdated)
	require.Nil(t, result.Findings[0].Line)
	require.Equal(t, 19, *result.Findings[0].OriginalLine)
}

func TestCollectorRetainsActiveReviewBodyFallback(t *testing.T) {
	t.Parallel()

	result := runCollector(t, "structured", false)
	require.Equal(t, "CHANGES_REQUESTED", result.ReviewDecision)
	require.Len(t, result.Findings, 2)
	require.Equal(t, "review-body-finding", result.Findings[0].Kind)
	require.Contains(t, result.Findings[0].Body, "First blocker")
	require.NotContains(t, result.Findings[0].Body, "Second blocker")
	require.Contains(t, result.Findings[1].Body, "Second blocker")
}

func TestCollectorDoesNotRetainHistoricalReviewBodyAfterApproval(t *testing.T) {
	t.Parallel()

	result := runCollector(t, "approved-body", false)
	require.Equal(t, "APPROVED", result.ReviewDecision)
	require.Empty(t, result.Findings)
}

func TestCollectorRefusesResultWhenHeadMovesDuringCollection(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	output, err := fixture.run(t, "current", true)
	require.Error(t, err, output)
	require.Contains(t, output, "PR head moved during collection")
	require.NoFileExists(t, fixture.output)
}

func runCollector(t *testing.T, mode string, moveHead bool) ledger {
	t.Helper()
	fixture := newFixture(t)
	output, err := fixture.run(t, mode, moveHead)
	require.NoError(t, err, output)
	var result ledger
	require.NoError(t, json.Unmarshal([]byte(readFile(t, fixture.output)), &result))
	require.Equal(t, 123, result.PRNumber)
	require.Equal(t, testHead, result.Head)

	return result
}

type fixture struct {
	root      string
	fakeBin   string
	runner    string
	output    string
	headReads string
}

func newFixture(t *testing.T) fixture {
	t.Helper()

	root := t.TempDir()
	fakeBin := filepath.Join(root, "bin")
	require.NoError(t, os.MkdirAll(fakeBin, 0o755))
	headReads := filepath.Join(root, "head-reads")
	writeFile(t, filepath.Join(fakeBin, "gh"), `#!/usr/bin/env bash
set -euo pipefail
case "$1 $2" in
  "repo view") printf 'owner/repo\n' ;;
  "pr view")
    count=0
    [[ ! -f "$TEST_HEAD_READS" ]] || count=$(cat "$TEST_HEAD_READS")
    count=$((count + 1))
    printf '%s' "$count" >"$TEST_HEAD_READS"
    if [[ "$count" -gt 1 && "${TEST_MOVE_HEAD:-false}" == "true" ]]; then
      printf '%s\n' "$TEST_MOVED_HEAD"
    else
      printf '%s\n' "$TEST_HEAD"
    fi
    ;;
  "api graphql")
    resolved=false
    outdated=false
    line=7
    original_line=7
    decision='"REVIEW_REQUIRED"'
    threads='[]'
    case "$TEST_MODE" in
      current) threads='[{"id":"THREAD_1","isResolved":false,"isOutdated":false,"path":"scripts/example","line":7,"originalLine":7,"comments":{"nodes":[{"databaseId":202,"url":"https://example/comment","body":"Untrusted review evidence","author":{"login":"reviewer"},"pullRequestReview":{"databaseId":101}}]}}]' ;;
      resolved) threads='[{"id":"THREAD_1","isResolved":true,"isOutdated":false,"path":"scripts/example","line":7,"originalLine":7,"comments":{"nodes":[{"databaseId":202,"url":"https://example/comment","body":"Resolved evidence","author":{"login":"reviewer"},"pullRequestReview":{"databaseId":101}}]}}]' ;;
      multiple) threads='[{"id":"THREAD_2","isResolved":false,"isOutdated":false,"path":"two.go","line":2,"originalLine":2,"comments":{"nodes":[{"databaseId":203,"url":"https://example/2","body":"Second","author":{"login":"two"},"pullRequestReview":{"databaseId":102}}]}},{"id":"THREAD_1","isResolved":false,"isOutdated":false,"path":"one.go","line":1,"originalLine":1,"comments":{"nodes":[{"databaseId":202,"url":"https://example/1","body":"First","author":{"login":"one"},"pullRequestReview":{"databaseId":101}}]}}]' ;;
      outdated) threads='[{"id":"THREAD_1","isResolved":false,"isOutdated":true,"path":"old.go","line":null,"originalLine":19,"comments":{"nodes":[{"databaseId":202,"url":"https://example/old","body":"Old line claim","author":{"login":"reviewer"},"pullRequestReview":{"databaseId":101}}]}}]' ;;
      structured) decision='"CHANGES_REQUESTED"' ;;
      approved-body) decision='"APPROVED"' ;;
    esac
    printf '[{"data":{"repository":{"pullRequest":{"reviewDecision":%s,"reviewThreads":{"nodes":%s,"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}]\n' "$decision" "$threads"
    ;;
  "api --paginate")
    body='DECISION: REQUEST CHANGES\n\n[P1][blocking] First blocker\nEvidence: first\n\n[P2][blocking] Second blocker\nEvidence: second'
    printf '[[{"id":101,"state":"CHANGES_REQUESTED","body":"%s","html_url":"https://example/review","user":{"login":"reviewer"}}]]\n' "$body"
    ;;
  *) exit 98 ;;
esac
`, 0o755)

	return fixture{
		root:      root,
		fakeBin:   fakeBin,
		runner:    sourcePath(t),
		output:    filepath.Join(root, "result", "known.json"),
		headReads: headReads,
	}
}

func (fixture fixture) run(t *testing.T, mode string, moveHead bool) (string, error) {
	t.Helper()

	command := exec.Command("bash", fixture.runner, "123", "--head", testHead, "--output", fixture.output)
	command.Dir = fixture.root
	command.Env = testenv.Environment(
		"PATH="+fixture.fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"TEST_HEAD="+testHead,
		"TEST_MOVED_HEAD="+testMovedHead,
		"TEST_HEAD_READS="+fixture.headReads,
		"TEST_MOVE_HEAD="+map[bool]string{true: "true", false: "false"}[moveHead],
		"TEST_MODE="+mode,
	)
	output, err := command.CombinedOutput()

	return string(output), err
}

func sourcePath(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)

	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "ai-pr-known-findings"))
}

func writeFile(t *testing.T, path, contents string, mode os.FileMode) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(contents), mode))
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)

	return strings.TrimSpace(string(contents))
}
