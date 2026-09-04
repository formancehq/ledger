package aiprloop

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/scripts/internal/testenv"
)

func TestHelpDoesNotRequirePRMetadata(t *testing.T) {
	t.Parallel()

	fakeBin := t.TempDir()
	writeExecutable(t, filepath.Join(fakeBin, "gh"), "#!/usr/bin/env bash\nexit 97\n")

	command := exec.Command("bash", launcherPath(t), "--help")
	command.Env = testenv.Environment("PATH=" + fakeBin + string(os.PathListSeparator) + os.Getenv("PATH"))
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "Usage: bash scripts/ai-pr-loop")
}

func TestLauncherUsesUniqueWorktreesImmutableBaseAndDirectReview(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	firstCapture := filepath.Join(fixture.root, "first-args")
	firstOutput, firstErr := runLauncher(t, fixture, firstCapture)
	require.NoError(t, firstErr, firstOutput)
	secondCapture := filepath.Join(fixture.root, "second-args")
	secondOutput, secondErr := runLauncher(t, fixture, secondCapture)
	require.NoError(t, secondErr, secondOutput)

	firstWorktree := worktreeFromOutput(t, firstOutput)
	secondWorktree := worktreeFromOutput(t, secondOutput)
	require.NotEqual(t, firstWorktree, secondWorktree)
	require.DirExists(t, firstWorktree)
	require.DirExists(t, secondWorktree)
	require.Equal(t, firstWorktree, strings.TrimSpace(readCapturedFile(t, firstCapture+".cwd")))
	require.Equal(t, firstWorktree, capturedArgument(t, firstCapture, "--worktree"))
	require.Equal(t, "123", capturedArgument(t, firstCapture, "--pr"))
	require.Equal(t, fixture.headSHA, capturedArgument(t, firstCapture, "--expected-head"))
	require.NotEqual(t, firstWorktree, capturedArgument(t, firstCapture, "--trusted-root"))
	require.NotEqual(t, firstWorktree, capturedArgument(t, firstCapture, "--validation-run-dir"))
	require.NotContains(t, capturedArgument(t, firstCapture, "--state-dir"), firstWorktree)
	require.Contains(t, capturedArgument(t, firstCapture, "--known-findings-cmd"), "trusted-tools/scripts/ai-pr-known-findings")
	require.NotEqual(t, firstWorktree, capturedArgument(t, firstCapture, "--known-findings-file"))
	require.Equal(t, fixture.baseSHA, baseArgument(t, firstCapture))
	require.Equal(t, fixture.baseSHA, baseArgument(t, secondCapture))
	require.Contains(t, capturedArgument(t, firstCapture, "--review-cmd"), "trusted-tools/scripts/ai-review-codex")
	require.NotContains(t, readCapturedFile(t, firstCapture), "--binding-file")
	require.NotContains(t, readCapturedFile(t, firstCapture), "--git-guard")
	require.Equal(t, 1, strings.Count(firstOutput, "ROOT_PROTECTION_ARMED"))
	require.Equal(t, 1, strings.Count(firstOutput, "ROOT_SNAPSHOT_CAPTURED position=after"))
	require.Equal(t, 1, strings.Count(firstOutput, "ROOT_UNCHANGED=PASS"))
	launcher := readCapturedFile(t, launcherPath(t))
	require.NotContains(t, launcher, "ai-pr-triage")
	require.NotContains(t, launcher, "codex-pr-triage")
	require.NotContains(t, launcher, "ai-fix-claude")
}

func TestLauncherCreatesCandidateWithoutMutatingDirtyRoot(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	dirtyPath := filepath.Join(fixture.checkout, "unrelated-user-file.txt")
	require.NoError(t, os.WriteFile(dirtyPath, []byte("keep me\n"), 0o644))
	statusBefore := runGitOutput(t, fixture.checkout, "status", "--porcelain=v1", "--untracked-files=all")
	capture := filepath.Join(fixture.root, "dirty-root-args")
	output, err := runLauncher(t, fixture, capture)
	require.NoError(t, err, output)

	worktree := worktreeFromOutput(t, output)
	require.NotEqual(t, fixture.checkout, worktree)
	require.Equal(t, worktree, strings.TrimSpace(readCapturedFile(t, capture+".cwd")))
	require.Equal(t, statusBefore, runGitOutput(t, fixture.checkout, "status", "--porcelain=v1", "--untracked-files=all"))
	require.Equal(t, "keep me\n", readCapturedFile(t, dirtyPath))
}

func TestLauncherSuppliesKnownFindingsToTheDirectReviewer(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	capture := filepath.Join(fixture.root, "review-args")
	output, err := runLauncher(t, fixture, capture)
	require.NoError(t, err, output)

	require.Contains(t, capturedArgument(t, capture, "--review-cmd"), "trusted-tools/scripts/ai-review-codex")
	ledgerPath := strings.TrimSpace(readCapturedFile(t, capture+".known"))
	require.NotEmpty(t, ledgerPath, "the reviewer must receive AI_REVIEW_KNOWN_FINDINGS")
	require.Equal(t, "123|"+fixture.headSHA, strings.TrimSpace(readCapturedFile(t, capture+".binding")))
	require.FileExists(t, strings.TrimSpace(readCapturedFile(t, capture+".context")))

	var ledger struct {
		PRNumber int    `json:"pr_number"`
		Head     string `json:"head"`
	}
	require.NoError(t, json.Unmarshal([]byte(readCapturedFile(t, ledgerPath)), &ledger))
	require.Equal(t, 123, ledger.PRNumber)
	require.Equal(t, fixture.headSHA, ledger.Head)
}

func TestLauncherAcceptsStructuredReviewBodyFindings(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	capture := filepath.Join(fixture.root, "review-args")
	// A review body split into blocker-level findings must reach the final
	// reviewer exactly like unresolved threads do.
	structured := `[{"id":"github-review-7-finding-1","kind":"review-body-finding",` +
		`"url":"https://github.com/owner/repo/pull/123#pullrequestreview-7","author":"reviewer",` +
		`"path":"","line":null,"original_line":null,"is_outdated":false,"body":"[P1][blocking] Structured blocking section"}]`
	output, err := runLauncher(t, fixture, capture,
		"TEST_KNOWN_FINDINGS_JSON="+structured)
	require.NoError(t, err, output)
	require.Contains(t, output, "1 unresolved GitHub review finding(s)")

	ledgerPath := strings.TrimSpace(readCapturedFile(t, capture+".known"))
	require.NotEmpty(t, ledgerPath, "the reviewer must receive AI_REVIEW_KNOWN_FINDINGS")
	require.Contains(t, readCapturedFile(t, ledgerPath), "github-review-7-finding-1")
}

func TestLauncherRefusesCollectedFindingWithoutIdentity(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	capture := filepath.Join(fixture.root, "review-args")
	// A mechanically collected item must retain a non-empty source identity.
	structured := `[{"id":"","kind":"review-body-finding","url":"","author":"reviewer",` +
		`"path":"","line":null,"original_line":null,"is_outdated":false,"body":"unstructured prose"}]`
	output, err := runLauncher(t, fixture, capture,
		"TEST_KNOWN_FINDINGS_JSON="+structured)
	require.Error(t, err, output)
	require.NoFileExists(t, capture, "technical review must not run")
	require.Contains(t, output, "AI_PR_LOOP_RESULT: REVIEW_FAILED")
}

func TestLauncherRefusesMalformedKnownFindingsLedger(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	capture := filepath.Join(fixture.root, "review-args")
	malformed := `[{"id":"github-review-1","kind":"review-body","url":"","author":"reviewer","path":"","line":null,"original_line":null,"is_outdated":false,"body":""}]`
	output, err := runLauncher(t, fixture, capture,
		"TEST_KNOWN_FINDINGS_JSON="+malformed)
	require.Error(t, err, output)
	require.NoFileExists(t, capture, "technical review must not run")
	require.Contains(t, output, "AI_PR_LOOP_RESULT: REVIEW_FAILED")
}

func TestLauncherRefusesKnownFindingsForAnotherTarget(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	capture := filepath.Join(fixture.root, "review-args")
	// A ledger snapshotted for another head cannot prove which blockers the
	// reviewed state still carries.
	output, err := runLauncher(t, fixture, capture,
		"TEST_KNOWN_FINDINGS_HEAD="+fixture.baseSHA)
	require.Error(t, err, output)
	require.NoFileExists(t, capture, "technical review must not run")
	require.Contains(t, output, "AI_PR_LOOP_RESULT: REVIEW_FAILED")
}

func TestOrdinaryToolingAndBugfixReachReviewWithoutTriage(t *testing.T) {
	for _, test := range []struct {
		name string
		env  []string
	}{
		{name: "tooling", env: []string{"TEST_PR_TITLE=chore: simplify tooling"}},
		{name: "bugfix", env: []string{
			"TEST_PR_TITLE=fix: repair tooling",
			"TEST_PR_BODY=DISCOVERY: NO_EXISTING_WORK\nBEFORE_FIX: BUG_REPRODUCED\nAFTER_FIX: PASS",
		}},
		{name: "scoped-bugfix", env: []string{
			"TEST_PR_TITLE=fix(wal): repair tooling",
			"TEST_PR_BODY=DISCOVERY: NO_EXISTING_WORK\nBEFORE_FIX: BUG_REPRODUCED\nAFTER_FIX: PASS",
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLauncherFixture(t)
			capture := filepath.Join(fixture.root, "review-args")
			output, err := runLauncher(t, fixture, capture, test.env...)
			require.NoError(t, err, output)
			require.FileExists(t, capture)
		})
	}
}

func TestScopedFixRequiresBugfixEvidence(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	capture := filepath.Join(fixture.root, "review-args")
	output, err := runLauncher(t, fixture, capture, "TEST_PR_TITLE=fix(wal): repair tooling")
	require.Error(t, err, output)
	require.NoFileExists(t, capture, "a scoped bugfix without evidence must not reach review")
	require.Contains(t, output, "AI_PR_LOOP_RESULT: ERROR (bugfix evidence rejected)")
}

func TestLauncherRemovesTheRunDirectoryOfACleanRun(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	capture := filepath.Join(fixture.root, "review-args")
	// Validation keeps only temporary state in the run directory. Shared caches
	// are external and must not become run-owned cleanup targets.
	output, err := runLauncherWithFlags(t, fixture, capture, nil,
		[]string{"TEST_RUN_VALIDATION_CMD=1"})
	require.NoError(t, err, output)
	require.Contains(t, output, "Worktree is clean.")

	runEntries := strings.Fields(readCapturedFile(t, capture+".rundir"))
	require.Equal(t, []string{"tmp"}, runEntries)
	require.NoDirExists(t, filepath.Dir(worktreeFromOutput(t, output)))
}

func TestFinalRootComparisonRunsForEveryReviewLoopOutcome(t *testing.T) {
	for _, status := range []string{"1", "2", "4"} {
		t.Run("exit "+status, func(t *testing.T) {
			fixture := newLauncherFixture(t)
			capture := filepath.Join(fixture.root, "review-args")
			output, err := runLauncher(t, fixture, capture, "TEST_REVIEW_LOOP_EXIT="+status)
			require.Error(t, err, output)
			require.Equal(t, 1, strings.Count(output, "ROOT_PROTECTION_ARMED"))
			require.Equal(t, 1, strings.Count(output, "ROOT_SNAPSHOT_CAPTURED position=after"))
			require.Equal(t, 1, strings.Count(output, "ROOT_UNCHANGED=PASS"))
		})
	}
}

type launcherFixture struct {
	root            string
	remote          string
	checkout        string
	fakeBin         string
	baseSHA         string
	advancedBaseSHA string
	headSHA         string
}

func newLauncherFixture(t *testing.T) launcherFixture {
	t.Helper()

	testRoot := t.TempDir()
	remote := filepath.Join(testRoot, "remote.git")
	seed := filepath.Join(testRoot, "seed")
	checkout := filepath.Join(testRoot, "checkout")

	runGit(t, testRoot, "init", "--bare", remote)
	require.NoError(t, os.MkdirAll(filepath.Join(seed, "scripts"), 0o755))
	runGit(t, seed, "init")
	runGit(t, seed, "config", "user.name", "AI PR Loop Test")
	runGit(t, seed, "config", "user.email", "ai-pr-loop@example.com")

	launcher, err := os.ReadFile(launcherPath(t))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "ai-pr-loop"), launcher, 0o755))
	revalidator, err := os.ReadFile(filepath.Join(filepath.Dir(launcherPath(t)), "ai-target-base-revalidate"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "ai-target-base-revalidate"), revalidator, 0o755))
	bugfixGate, err := os.ReadFile(filepath.Join(filepath.Dir(launcherPath(t)), "ai-bugfix-gate"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "ai-bugfix-gate"), bugfixGate, 0o755))
	writeExecutable(t, filepath.Join(seed, "scripts", "agent-validation-env"), `#!/usr/bin/env bash
set -euo pipefail
run_dir=$1
shift
mkdir -p "$run_dir/tmp"
exec "$@"
`)
	writeExecutable(t, filepath.Join(seed, "scripts", "agent-check-pr"), `#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--list" ]]; then printf 'agent-check\n'; fi
`)
	writeExecutable(t, filepath.Join(seed, "scripts", "agent-just"), "#!/usr/bin/env bash\nexit 0\n")
	writeExecutable(t, filepath.Join(seed, "scripts", "review-loop"), `#!/usr/bin/env bash
set -euo pipefail
arguments=("$@")
validation_cmd=""
known_findings_cmd=""
validation_run_dir=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --validation-cmd) validation_cmd=$2; shift 2 ;;
        --known-findings-cmd) known_findings_cmd=$2; shift 2 ;;
        --validation-run-dir) validation_run_dir=$2; shift 2 ;;
        *) shift ;;
    esac
done
bash -c "$validation_cmd"
bash -c "$known_findings_cmd"
jq -e --argjson pr "$AI_REVIEW_KNOWN_FINDINGS_PR" --arg head "$AI_REVIEW_KNOWN_FINDINGS_HEAD" \
    '.version == 1 and .pr_number == $pr and .head == $head and (.findings | type == "array")' \
    "$AI_REVIEW_KNOWN_FINDINGS" >/dev/null
set -- "${arguments[@]}"
printf '%s\n' "$@" > "$TEST_CAPTURE_FILE"
pwd > "$TEST_CAPTURE_FILE.cwd"
printf '%s\n' "${AI_REVIEW_KNOWN_FINDINGS:-}" > "$TEST_CAPTURE_FILE.known"
printf '%s|%s\n' "${AI_REVIEW_KNOWN_FINDINGS_PR:-}" "${AI_REVIEW_KNOWN_FINDINGS_HEAD:-}" > "$TEST_CAPTURE_FILE.binding"
printf '%s\n' "${AI_REVIEW_CONTEXT:-}" > "$TEST_CAPTURE_FILE.context"
if [[ -n "${TEST_RUN_VALIDATION_CMD:-}" ]]; then
    ls -1 "$validation_run_dir" > "$TEST_CAPTURE_FILE.rundir"
fi
if [[ "${TEST_ADVANCE_TARGET_AFTER_REVIEW:-false}" == "true" ]]; then
    git --git-dir="$TEST_REMOTE" update-ref refs/heads/release/v3.0 "$TEST_ADVANCED_BASE_SHA"
fi
exit "${TEST_REVIEW_LOOP_EXIT:-0}"
`)
	writeExecutable(t, filepath.Join(seed, "scripts", "rootguard"), `#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == --root && "$3" == -- ]]
shift 3
echo "ROOT_PROTECTION_ARMED gitProcesses=6 ignoredEntries=0"
set +e
"$@"
status=$?
set -e
echo "ROOT_SNAPSHOT_CAPTURED position=after gitProcesses=6 ignoredEntries=0"
echo "ROOT_UNCHANGED=PASS"
exit "$status"
`)
	writeExecutable(t, filepath.Join(seed, "scripts", "ai-review-codex"), "#!/usr/bin/env bash\nexit 0\n")
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "ai-pr-known-findings"), []byte(`#!/usr/bin/env bash
set -euo pipefail
pr=$1
shift
head=""
output=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --head) head=$2; shift 2 ;;
        --output) output=$2; shift 2 ;;
        *) shift ;;
    esac
done
[[ -n "$head" && -n "$output" ]]
printf '{"version":1,"pr_number":%s,"head":"%s","review_decision":"REVIEW_REQUIRED","findings":%s}\n' \
	"$pr" "${TEST_KNOWN_FINDINGS_HEAD:-$head}" "${TEST_KNOWN_FINDINGS_JSON:-[]}" > "$output"
jq -e 'all(.findings[]; (.id | length) > 0 and (.body | length) > 0)' "$output" >/dev/null
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(seed, "base.txt"), []byte("base\n"), 0o644))
	runGit(t, seed, "add", ".")
	runGit(t, seed, "commit", "-m", "base")
	runGit(t, seed, "branch", "-M", "release/v3.0")
	baseSHA := runGitOutput(t, seed, "rev-parse", "HEAD")
	runGit(t, seed, "remote", "add", "origin", remote)
	runGit(t, seed, "push", "-u", "origin", "release/v3.0")
	require.NoError(t, os.WriteFile(filepath.Join(seed, "advanced-base.txt"), []byte("advanced base\n"), 0o644))
	runGit(t, seed, "add", "advanced-base.txt")
	runGit(t, seed, "commit", "-m", "advance target fixture")
	advancedBaseSHA := runGitOutput(t, seed, "rev-parse", "HEAD")
	runGit(t, seed, "push", "origin", "release/v3.0")
	runGit(t, seed, "reset", "--hard", baseSHA)
	runGit(t, seed, "push", "--force", "origin", "release/v3.0")

	runGit(t, seed, "checkout", "-b", "feature")
	require.NoError(t, os.WriteFile(filepath.Join(seed, "feature.txt"), []byte("feature\n"), 0o644))
	writeExecutable(t, filepath.Join(seed, "scripts", "review-loop"), "#!/usr/bin/env bash\nexit 97\n")
	runGit(t, seed, "add", "feature.txt", "scripts/review-loop")
	runGit(t, seed, "commit", "-m", "feature")
	headSHA := runGitOutput(t, seed, "rev-parse", "HEAD")
	runGit(t, seed, "push", "-u", "origin", "feature")
	runGit(t, testRoot, "clone", "--branch", "release/v3.0", remote, checkout)

	fakeBin := filepath.Join(testRoot, "bin")
	require.NoError(t, os.MkdirAll(fakeBin, 0o755))
	writeExecutable(t, filepath.Join(fakeBin, "gh"), `#!/usr/bin/env bash
set -euo pipefail
case "$1 $2" in
    "repo view") printf 'owner/repo\n' ;;
    "pr view")
        jq -n --arg base "$TEST_BASE_SHA" --arg head "$TEST_HEAD_SHA" --arg title "${TEST_PR_TITLE:-chore: tooling}" --arg body "${TEST_PR_BODY:-}" '{number:123,url:"https://github.com/owner/repo/pull/123",state:"OPEN",isDraft:false,title:$title,body:$body,labels:[],baseRefName:"release/v3.0",baseRefOid:$base,headRefName:"feature",headRefOid:$head,headRepositoryOwner:{login:"owner"},headRepository:{name:"repo"}}'
        ;;
    *) exit 98 ;;
esac
`)
	writeExecutable(t, filepath.Join(fakeBin, "nix"), `#!/usr/bin/env bash
set -euo pipefail
if [[ " $* " != *" ./scripts/reviewloop"* ]]; then
    eval "${@: -1}"
    exit $?
fi
source_root=${@: -3:1}
review_output=${@: -2:1}
rootguard_output=${@: -1}
cp "$source_root/scripts/review-loop" "$review_output"
cp "$source_root/scripts/rootguard" "$rootguard_output"
chmod 755 "$review_output" "$rootguard_output"
`)

	return launcherFixture{root: testRoot, remote: remote, checkout: checkout, fakeBin: fakeBin, baseSHA: baseSHA, advancedBaseSHA: advancedBaseSHA, headSHA: headSHA}
}

func launcherPath(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "ai-pr-loop"))
	require.NoError(t, err)

	return path
}

func runLauncher(t *testing.T, fixture launcherFixture, capturePath string, extraEnv ...string) (string, error) {
	t.Helper()

	return runLauncherWithFlags(t, fixture, capturePath, []string{"--keep-worktree"}, extraEnv)
}

func runLauncherWithFlags(
	t *testing.T,
	fixture launcherFixture,
	capturePath string,
	flags []string,
	extraEnv []string,
) (string, error) {
	t.Helper()
	arguments := append([]string{filepath.Join(fixture.checkout, "scripts", "ai-pr-loop"), "123"}, flags...)
	command := exec.Command("bash", arguments...)
	command.Dir = fixture.checkout
	environment := []string{
		"PATH=" + fixture.fakeBin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"TEST_BASE_SHA=" + fixture.baseSHA,
		"TEST_HEAD_SHA=" + fixture.headSHA,
		"TEST_REMOTE=" + fixture.remote,
		"TEST_ADVANCED_BASE_SHA=" + fixture.advancedBaseSHA,
		"TEST_CAPTURE_FILE=" + capturePath,
	}
	environment = append(environment, extraEnv...)
	command.Env = testenv.Environment(environment...)
	output, err := command.CombinedOutput()

	return string(output), err
}

func worktreeFromOutput(t *testing.T, output string) string {
	t.Helper()
	for line := range strings.SplitSeq(output, "\n") {
		if worktree, found := strings.CutPrefix(strings.TrimSpace(line), "worktree: "); found {
			return worktree
		}
	}
	t.Fatalf("worktree path not found in output:\n%s", output)

	return ""
}

func baseArgument(t *testing.T, capturePath string) string {
	t.Helper()

	return capturedArgument(t, capturePath, "--base")
}

// capturedArgument reads one flag value from the captured argument list. The
// review-loop fake records one argument per line, so values containing spaces
// stay intact.
func capturedArgument(t *testing.T, capturePath, name string) string {
	t.Helper()
	content := readCapturedFile(t, capturePath)
	arguments := strings.Split(strings.TrimSuffix(content, "\n"), "\n")
	for index, argument := range arguments {
		if argument == name && index+1 < len(arguments) {
			return arguments[index+1]
		}
	}
	t.Fatalf("%s not found in captured arguments: %q", name, content)

	return ""
}

func readCapturedFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	require.NoError(t, err)

	return string(content)
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o755))
}

func runGit(t *testing.T, directory string, arguments ...string) {
	t.Helper()
	_ = runGitOutput(t, directory, arguments...)
}

func runGitOutput(t *testing.T, directory string, arguments ...string) string {
	t.Helper()
	command := testenv.Command(t, "git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, fmt.Sprintf("git %s:\n%s", strings.Join(arguments, " "), output))

	return strings.TrimSpace(string(output))
}
