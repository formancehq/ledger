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
)

func TestHelpDoesNotRequirePRMetadata(t *testing.T) {
	t.Parallel()

	fakeBin := t.TempDir()
	writeExecutable(t, filepath.Join(fakeBin, "gh"), "#!/usr/bin/env bash\nexit 97\n")

	command := exec.Command("bash", launcherPath(t), "--help")
	command.Env = append(os.Environ(), "PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "Usage: bash scripts/ai-pr-loop")
}

func TestLauncherUsesUniqueWorktreesImmutableBaseAndKeepTriage(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	firstCapture := filepath.Join(fixture.root, "first-args")
	firstOutput, firstErr := runLauncher(t, fixture, firstCapture, triageResult{decision: "KEEP"})
	require.NoError(t, firstErr, firstOutput)
	secondCapture := filepath.Join(fixture.root, "second-args")
	secondOutput, secondErr := runLauncher(t, fixture, secondCapture, triageResult{decision: "KEEP"})
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
	require.NotEqual(t, firstWorktree, capturedArgument(t, firstCapture, "--validation-tool-root"))
	require.Contains(t, capturedArgument(t, firstCapture, "--validation-tool-root"), "trusted-tools")
	require.NotContains(t, capturedArgument(t, firstCapture, "--state-dir"), firstWorktree)
	require.Contains(t, capturedArgument(t, firstCapture, "--validation-gates-cmd"), "agent-check-pr --list")
	require.Equal(t, fixture.baseSHA, baseArgument(t, firstCapture))
	require.Equal(t, fixture.baseSHA, baseArgument(t, secondCapture))
	require.Contains(t, firstOutput, "legitimacy triage KEEP")
}

func TestLauncherCreatesCandidateWithoutMutatingDirtyRoot(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	dirtyPath := filepath.Join(fixture.checkout, "unrelated-user-file.txt")
	require.NoError(t, os.WriteFile(dirtyPath, []byte("keep me\n"), 0o644))
	statusBefore := runGitOutput(t, fixture.checkout, "status", "--porcelain=v1", "--untracked-files=all")
	capture := filepath.Join(fixture.root, "dirty-root-args")
	output, err := runLauncher(t, fixture, capture, triageResult{decision: "KEEP"})
	require.NoError(t, err, output)

	worktree := worktreeFromOutput(t, output)
	require.NotEqual(t, fixture.checkout, worktree)
	require.Equal(t, worktree, strings.TrimSpace(readCapturedFile(t, capture+".cwd")))
	require.Equal(t, statusBefore, runGitOutput(t, fixture.checkout, "status", "--porcelain=v1", "--untracked-files=all"))
	require.Equal(t, "keep me\n", readCapturedFile(t, dirtyPath))
}

func TestLauncherStopsBeforeReviewForQuestionAndReject(t *testing.T) {
	for _, decision := range []string{"QUESTION", "REJECT"} {
		t.Run(decision, func(t *testing.T) {
			fixture := newLauncherFixture(t)
			capture := filepath.Join(fixture.root, "review-args")
			output, err := runLauncher(t, fixture, capture, triageResult{decision: decision})
			require.Error(t, err, output)
			require.NoFileExists(t, capture, "technical review must not run")
			if decision == "QUESTION" {
				require.Contains(t, output, "AI_PR_LOOP_RESULT: HUMAN_DECISION_REQUIRED")
			} else {
				require.Contains(t, output, "AI_PR_LOOP_RESULT: LEGITIMACY_REJECTED")
			}
		})
	}
}

func TestLauncherRefusesTriageResultForAnotherTarget(t *testing.T) {
	for _, moved := range []string{"head", "base"} {
		t.Run(moved, func(t *testing.T) {
			fixture := newLauncherFixture(t)
			// A base-pinned triage tool that ignores the launcher-provided
			// expected SHAs can answer for a PR state the launcher never
			// fetched.
			triage := triageResult{decision: "KEEP"}
			if moved == "head" {
				triage.headSHA = fixture.baseSHA
			} else {
				triage.baseSHA = fixture.headSHA
			}
			capture := filepath.Join(fixture.root, "review-args")
			output, err := runLauncher(t, fixture, capture, triage)
			require.Error(t, err, output)
			require.NoFileExists(t, capture, "technical review must not run")
			require.Contains(t, output, "AI_PR_LOOP_RESULT: ERROR (triage target mismatch)")
		})
	}
}

func TestLauncherReconcilesKnownFindingsBoundToTheVerifiedTarget(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	capture := filepath.Join(fixture.root, "review-args")
	output, err := runLauncher(t, fixture, capture, triageResult{decision: "KEEP"})
	require.NoError(t, err, output)

	require.Contains(t, capturedArgument(t, capture, "--review-cmd"), "trusted-tools/scripts/ai-review-known-findings")
	ledgerPath := strings.TrimSpace(readCapturedFile(t, capture+".known"))
	require.NotEmpty(t, ledgerPath, "the reviewer must receive AI_REVIEW_KNOWN_FINDINGS")
	require.Equal(t, "123|"+fixture.headSHA, strings.TrimSpace(readCapturedFile(t, capture+".binding")))

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
	// A review body split into blocker-level findings must reach reconciliation
	// exactly like inline comments do.
	structured := `[{"id":"github-review-7-finding-1","kind":"review-body-finding","source_review_id":7,` +
		`"source_id":7,"url":"https://github.com/owner/repo/pull/123#pullrequestreview-7","author":"reviewer",` +
		`"path":"","line":null,"body":"[P1][blocking] Structured blocking section"}]`
	output, err := runLauncher(t, fixture, capture, triageResult{decision: "KEEP"},
		"TEST_KNOWN_FINDINGS_JSON="+structured)
	require.NoError(t, err, output)
	require.Contains(t, output, "1 known blocking GitHub finding(s)")

	ledgerPath := strings.TrimSpace(readCapturedFile(t, capture+".known"))
	require.NotEmpty(t, ledgerPath, "the reviewer must receive AI_REVIEW_KNOWN_FINDINGS")
	require.Contains(t, readCapturedFile(t, ledgerPath), "github-review-7-finding-1")
}

func TestLauncherRefusesStructuredFindingWithoutBlockerMarker(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	capture := filepath.Join(fixture.root, "review-args")
	// The structured kind is only meaningful for bodies that actually start at a
	// blocker marker, so its identity and body must stay verifiable.
	structured := `[{"id":"github-review-7-finding-1","kind":"review-body-finding","source_review_id":7,` +
		`"source_id":7,"url":"","author":"reviewer","path":"","line":null,"body":"unstructured prose"}]`
	output, err := runLauncher(t, fixture, capture, triageResult{decision: "KEEP"},
		"TEST_KNOWN_FINDINGS_JSON="+structured)
	require.Error(t, err, output)
	require.NoFileExists(t, capture, "technical review must not run")
	require.Contains(t, output, "AI_PR_LOOP_RESULT: ERROR (known findings target mismatch)")
}

func TestLauncherRefusesMalformedKnownFindingsLedger(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	capture := filepath.Join(fixture.root, "review-args")
	malformed := `[{"id":"github-review-1","kind":"review-body","source_review_id":1,"source_id":1,"url":"","author":"reviewer","path":"","line":null,"body":""}]`
	output, err := runLauncher(t, fixture, capture, triageResult{decision: "KEEP"},
		"TEST_KNOWN_FINDINGS_JSON="+malformed)
	require.Error(t, err, output)
	require.NoFileExists(t, capture, "technical review must not run")
	require.Contains(t, output, "AI_PR_LOOP_RESULT: ERROR (known findings target mismatch)")
}

func TestLauncherRefusesKnownFindingsForAnotherTarget(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	capture := filepath.Join(fixture.root, "review-args")
	// A ledger snapshotted for another head cannot prove which blockers the
	// reviewed state still carries.
	output, err := runLauncher(t, fixture, capture, triageResult{decision: "KEEP"},
		"TEST_KNOWN_FINDINGS_HEAD="+fixture.baseSHA)
	require.Error(t, err, output)
	require.NoFileExists(t, capture, "technical review must not run")
	require.Contains(t, output, "AI_PR_LOOP_RESULT: ERROR (known findings target mismatch)")
}

func TestLauncherRemovesTheRunDirectoryOfACleanRun(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	capture := filepath.Join(fixture.root, "review-args")
	// Validation isolates HOME and the Go caches inside the run directory, so a
	// completed run without --keep-worktree must reclaim that whole directory.
	output, err := runLauncherWithFlags(t, fixture, capture, triageResult{decision: "KEEP"}, nil,
		[]string{"TEST_RUN_VALIDATION_CMD=1"})
	require.NoError(t, err, output)
	require.Contains(t, output, "Worktree is clean.")

	isolatedCaches := readCapturedFile(t, capture+".rundir")
	for _, cache := range []string{"home", "go-cache", "go-mod-cache", "go-path", "tmp", "cache"} {
		require.Contains(t, strings.Split(isolatedCaches, "\n"), cache,
			"validation must have populated the run directory")
	}
	require.NoDirExists(t, filepath.Dir(worktreeFromOutput(t, output)))
}

func TestLauncherTreatsTriageFailureAsOrchestrationError(t *testing.T) {
	fixture := newLauncherFixture(t)
	capture := filepath.Join(fixture.root, "review-args")
	output, err := runLauncher(t, fixture, capture, triageResult{exitCode: 2})
	require.Error(t, err, output)
	var exitError *exec.ExitError
	require.ErrorAs(t, err, &exitError)
	require.Equal(t, 1, exitError.ExitCode(), output)
	require.NoFileExists(t, capture, "technical review must not run")
	require.Contains(t, output, "AI_PR_LOOP_RESULT: ERROR (triage exit 2)")
}

func TestLauncherClassifiesGenuinelyMissingSharedPolicyAsToolingError(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	updater := filepath.Join(fixture.root, "missing-policy-base")
	runGit(t, fixture.root, "clone", "--branch", "release/v3.0", filepath.Join(fixture.root, "remote.git"), updater)
	runGit(t, updater, "config", "user.name", "Target Branch Update")
	runGit(t, updater, "config", "user.email", "target-update@example.com")
	runGit(t, updater, "rm", "scripts/ai-pr-publication-preconditions")
	runGit(t, updater, "commit", "-m", "remove publication preconditions")
	runGit(t, updater, "push", "origin", "release/v3.0")
	fixture.baseSHA = runGitOutput(t, updater, "rev-parse", "HEAD")

	capture := filepath.Join(fixture.root, "review-args")
	output, err := runLauncher(t, fixture, capture, triageResult{decision: "KEEP"})
	require.Error(t, err, output)
	var exitError *exec.ExitError
	require.ErrorAs(t, err, &exitError)
	require.Equal(t, 1, exitError.ExitCode(), output)
	require.Contains(t, output, "AI_PR_LOOP_RESULT: TOOLING_ERROR (missing publication preconditions)")
	require.NotContains(t, output, "HUMAN_DECISION_REQUIRED")
	require.NoFileExists(t, capture, "technical review must not run")
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
	guard, err := os.ReadFile(filepath.Join(filepath.Dir(launcherPath(t)), "ai-git-guard"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "ai-git-guard"), guard, 0o755))
	preconditions, err := os.ReadFile(filepath.Join(filepath.Dir(launcherPath(t)), "ai-pr-publication-preconditions"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "ai-pr-publication-preconditions"), preconditions, 0o755))
	bugfixGate, err := os.ReadFile(filepath.Join(filepath.Dir(launcherPath(t)), "ai-bugfix-gate"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "ai-bugfix-gate"), bugfixGate, 0o755))
	writeExecutable(t, filepath.Join(seed, "scripts", "review-loop"), `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "$TEST_CAPTURE_FILE"
pwd > "$TEST_CAPTURE_FILE.cwd"
printf '%s\n' "${AI_REVIEW_KNOWN_FINDINGS:-}" > "$TEST_CAPTURE_FILE.known"
printf '%s|%s\n' "${AI_REVIEW_KNOWN_FINDINGS_PR:-}" "${AI_REVIEW_KNOWN_FINDINGS_HEAD:-}" > "$TEST_CAPTURE_FILE.binding"
if [[ -n "${TEST_RUN_VALIDATION_CMD:-}" ]]; then
    validation_cmd=""
    validation_run_dir=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --validation-cmd) validation_cmd=$2; shift 2 ;;
            --validation-run-dir) validation_run_dir=$2; shift 2 ;;
            *) shift ;;
        esac
    done
    # review-loop runs the validation recipe through bash -lc from the worktree.
    # The recipe first creates the isolated caches in the parent run directory,
    # then invokes the trusted validator, which this fixture does not provide.
    bash -lc "$validation_cmd" >/dev/null 2>&1 || true
    ls -1 "$validation_run_dir" > "$TEST_CAPTURE_FILE.rundir"
fi
if [[ "${TEST_ADVANCE_TARGET_AFTER_REVIEW:-false}" == "true" ]]; then
    git --git-dir="$TEST_REMOTE" update-ref refs/heads/release/v3.0 "$TEST_ADVANCED_BASE_SHA"
fi
`)
	writeExecutable(t, filepath.Join(seed, "scripts", "ai-review-codex"), "#!/usr/bin/env bash\nexit 0\n")
	writeExecutable(t, filepath.Join(seed, "scripts", "ai-review-known-findings"), "#!/usr/bin/env bash\nexit 0\n")
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
printf '{"version":1,"pr_number":%s,"head":"%s","findings":%s}\n' \
	"$pr" "${TEST_KNOWN_FINDINGS_HEAD:-$head}" "${TEST_KNOWN_FINDINGS_JSON:-[]}" > "$output"
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "ai-pr-triage"), []byte(`#!/usr/bin/env bash
set -euo pipefail
[[ "${AI_PR_TRIAGE_EXPECT_BASE_SHA:-}" == "$TEST_BASE_SHA" ]]
[[ "${AI_PR_TRIAGE_EXPECT_HEAD_SHA:-}" == "$TEST_HEAD_SHA" ]]
if [[ "$TEST_TRIAGE_EXIT_CODE" -ne 0 ]]; then
    exit "$TEST_TRIAGE_EXIT_CODE"
fi
output=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --output) output=$2; shift 2 ;;
        *) shift ;;
    esac
done
[[ -n "$output" ]]
printf '{"decision":"%s","base_sha":"%s","head":"%s"}\n' \
    "$TEST_TRIAGE_DECISION" "$TEST_TRIAGE_BASE_SHA" "$TEST_TRIAGE_HEAD_SHA" > "$output"
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
	writeExecutable(t, filepath.Join(seed, "scripts", "ai-review-known-findings"), "#!/usr/bin/env bash\nexit 97\n")
	runGit(t, seed, "add", "feature.txt", "scripts/review-loop", "scripts/ai-review-known-findings")
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
        printf '{"number":123,"url":"https://github.com/owner/repo/pull/123","state":"OPEN","isDraft":false,"baseRefName":"release/v3.0","baseRefOid":"%s","headRefName":"feature","headRefOid":"%s","headRepositoryOwner":{"login":"owner"},"headRepository":{"name":"repo"}}\n' "$TEST_BASE_SHA" "$TEST_HEAD_SHA"
        ;;
    *) exit 98 ;;
esac
`)
	writeExecutable(t, filepath.Join(fakeBin, "nix"), `#!/usr/bin/env bash
set -euo pipefail
source_root=""
output=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -C)
            source_root=$2
            shift 2
            ;;
        -o)
            output=$2
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done
[[ -n "$source_root" && -n "$output" ]]
cp "$source_root/scripts/review-loop" "$output"
chmod 755 "$output"
`)

	return launcherFixture{root: testRoot, remote: remote, checkout: checkout, fakeBin: fakeBin, baseSHA: baseSHA, advancedBaseSHA: advancedBaseSHA, headSHA: headSHA}
}

func launcherPath(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "ai-pr-loop"))
	require.NoError(t, err)

	return path
}

// triageResult is the result the fake triage tool reports. Empty SHAs mean the
// tool answers for the exact target the launcher verified.
type triageResult struct {
	decision string
	baseSHA  string
	headSHA  string
	exitCode int
}

func runLauncher(t *testing.T, fixture launcherFixture, capturePath string, triage triageResult, extraEnv ...string) (string, error) {
	t.Helper()

	return runLauncherWithFlags(t, fixture, capturePath, triage, []string{"--keep-worktree"}, extraEnv)
}

func runLauncherWithFlags(
	t *testing.T,
	fixture launcherFixture,
	capturePath string,
	triage triageResult,
	flags []string,
	extraEnv []string,
) (string, error) {
	t.Helper()
	if triage.baseSHA == "" {
		triage.baseSHA = fixture.baseSHA
	}
	if triage.headSHA == "" {
		triage.headSHA = fixture.headSHA
	}
	arguments := append([]string{filepath.Join(fixture.checkout, "scripts", "ai-pr-loop"), "123"}, flags...)
	command := exec.Command("bash", arguments...)
	command.Dir = fixture.checkout
	command.Env = append(os.Environ(),
		"PATH="+fixture.fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"TEST_BASE_SHA="+fixture.baseSHA,
		"TEST_HEAD_SHA="+fixture.headSHA,
		"TEST_REMOTE="+fixture.remote,
		"TEST_ADVANCED_BASE_SHA="+fixture.advancedBaseSHA,
		"TEST_CAPTURE_FILE="+capturePath,
		"TEST_TRIAGE_DECISION="+triage.decision,
		"TEST_TRIAGE_BASE_SHA="+triage.baseSHA,
		"TEST_TRIAGE_HEAD_SHA="+triage.headSHA,
		fmt.Sprintf("TEST_TRIAGE_EXIT_CODE=%d", triage.exitCode),
	)
	command.Env = append(command.Env, extraEnv...)
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
	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, fmt.Sprintf("git %s:\n%s", strings.Join(arguments, " "), output))

	return strings.TrimSpace(string(output))
}
