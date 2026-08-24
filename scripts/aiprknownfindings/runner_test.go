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
)

const (
	testHead      = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testMovedHead = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestCollectorPublishesStableFindingsForExactHead(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	output, err := fixture.run(t, false)
	require.NoError(t, err, output)
	require.Contains(t, output, "AI_PR_KNOWN_FINDINGS_COUNT: 1")

	var ledger struct {
		PRNumber int `json:"pr_number"`
		Head     string
		Findings []struct {
			ID   string
			Kind string
			Body string
		}
	}
	require.NoError(t, json.Unmarshal([]byte(readFile(t, fixture.output)), &ledger))
	require.Equal(t, 123, ledger.PRNumber)
	require.Equal(t, testHead, ledger.Head)
	require.Len(t, ledger.Findings, 1)
	require.Equal(t, "github-review-comment-202", ledger.Findings[0].ID)
	require.Equal(t, "inline-review-comment", ledger.Findings[0].Kind)
	require.Equal(t, "Untrusted review evidence", ledger.Findings[0].Body)
}

func TestCollectorRefusesResultWhenHeadMovesDuringSnapshot(t *testing.T) {
	t.Parallel()

	fixture := newFixture(t)
	output, err := fixture.run(t, true)
	require.Error(t, err, output)
	require.Contains(t, output, "PR head moved during snapshot")
	require.NoFileExists(t, fixture.output)
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
  "repo view")
    printf 'owner/repo\n'
    ;;
  "pr view")
    count=0
    if [[ -f "$TEST_HEAD_READS" ]]; then count=$(cat "$TEST_HEAD_READS"); fi
    count=$((count + 1))
    printf '%s' "$count" > "$TEST_HEAD_READS"
    if [[ "$count" -gt 1 && "${TEST_MOVE_HEAD:-false}" == "true" ]]; then
      printf '%s\n' "$TEST_MOVED_HEAD"
    else
      printf '%s\n' "$TEST_HEAD"
    fi
    ;;
  "api --paginate")
    case "$3" in
      *'/reviews?'*)
        printf '[{"id":101,"state":"CHANGES_REQUESTED","body":"Blocking review","html_url":"https://example/review","user":{"login":"reviewer"}}]\n'
        ;;
      *'/comments?'*)
        printf '[{"id":202,"pull_request_review_id":101,"html_url":"https://example/comment","user":{"login":"reviewer"},"path":"scripts/example","line":7,"body":"Untrusted review evidence"}]\n'
        ;;
      *) exit 97 ;;
    esac
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

func (fixture fixture) run(t *testing.T, moveHead bool) (string, error) {
	t.Helper()

	command := exec.Command("bash", fixture.runner, "123", "--head", testHead, "--output", fixture.output)
	command.Dir = fixture.root
	command.Env = append(os.Environ(),
		"PATH="+fixture.fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"TEST_HEAD="+testHead,
		"TEST_MOVED_HEAD="+testMovedHead,
		"TEST_HEAD_READS="+fixture.headReads,
		"TEST_MOVE_HEAD="+map[bool]string{true: "true", false: "false"}[moveHead],
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
