package aiprloop

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/scripts/internal/testenv"
)

func TestPushReviewsCandidateCommitBeforePublishing(t *testing.T) {
	t.Parallel()

	fixture := newPushFixture(t, pushFixtureOptions{})
	output, exitCode := fixture.run(t)
	require.Equal(t, 0, exitCode, output)
	require.Contains(t, output, "AI_PR_LOOP_PUSH_RESULT: PUSHED")

	remoteHead := runGitOutput(t, fixture.root, "--git-dir", fixture.remote, "rev-parse", "refs/heads/feature")
	require.NotEqual(t, fixture.headSHA, remoteHead)
	require.Equal(t, fixture.headSHA, runGitOutput(t, fixture.root, "--git-dir", fixture.remote, "rev-parse", remoteHead+"^"))

	countBytes, err := os.ReadFile(fixture.reviewCountFile)
	require.NoError(t, err)
	require.Equal(t, "2", strings.TrimSpace(string(countBytes)))
}

func TestPushRefusesWhenRemoteHeadMoves(t *testing.T) {
	t.Parallel()

	fixture := newPushFixture(t, pushFixtureOptions{moveRemote: true})
	output, exitCode := fixture.run(t)
	require.Equal(t, 2, exitCode, output)
	require.Contains(t, output, "AI_PR_LOOP_PUSH_RESULT: REFUSED (remote PR head moved)")

	remoteHead := runGitOutput(t, fixture.root, "--git-dir", fixture.remote, "rev-parse", "refs/heads/feature")
	require.Equal(t, fixture.baseSHA, remoteHead)
}

func TestPushRefusesWhenReviewedCandidateHeadMoves(t *testing.T) {
	t.Parallel()

	fixture := newPushFixture(t, pushFixtureOptions{moveLocalHead: true})
	output, exitCode := fixture.run(t)
	require.Equal(t, 1, exitCode, output)
	require.Contains(t, output, "AI_PR_LOOP_PUSH_RESULT: REFUSED (reviewed candidate HEAD changed)")

	remoteHead := runGitOutput(t, fixture.root, "--git-dir", fixture.remote, "rev-parse", "refs/heads/feature")
	require.Equal(t, fixture.headSHA, remoteHead)
}

func TestPushUsesBasePinnedReviewToolchain(t *testing.T) {
	t.Parallel()

	fixture := newPushFixture(t, pushFixtureOptions{tamperTargetToolchain: true})
	output, exitCode := fixture.run(t)
	require.Equal(t, 0, exitCode, output)
	require.Contains(t, output, "AI_PR_LOOP_PUSH_RESULT: PUSHED")
}

func TestPushRefusesBaseWithoutTrustedValidator(t *testing.T) {
	t.Parallel()

	fixture := newPushFixture(t, pushFixtureOptions{omitTrustedValidator: true})
	output, exitCode := fixture.run(t)
	require.Equal(t, 1, exitCode, output)
	require.Contains(t, output, "trusted base does not provide readable non-symlink regular file scripts/agent-check-pr")

	remoteHead := runGitOutput(t, fixture.root, "--git-dir", fixture.remote, "rev-parse", "refs/heads/feature")
	require.Equal(t, fixture.headSHA, remoteHead)
}

func TestPushAcceptsNonExecutableBasePinnedPublicationScripts(t *testing.T) {
	t.Parallel()

	fixture := newPushFixture(t, pushFixtureOptions{trustedPublicationToolsNonExecutable: true})
	output, exitCode := fixture.run(t)
	require.Equal(t, 0, exitCode, output)
	require.Contains(t, output, "AI_PR_LOOP_PUSH_RESULT: PUSHED")
}

func TestPushRefusesNonRegularBasePinnedPublicationScript(t *testing.T) {
	t.Parallel()

	fixture := newPushFixture(t, pushFixtureOptions{trustedValidatorDirectory: true})
	output, exitCode := fixture.run(t)
	require.Equal(t, 1, exitCode, output)
	require.Contains(t, output, "trusted base does not provide readable non-symlink regular file scripts/agent-check-pr")

	remoteHead := runGitOutput(t, fixture.root, "--git-dir", fixture.remote, "rev-parse", "refs/heads/feature")
	require.Equal(t, fixture.headSHA, remoteHead)
}

func TestPushRefusesSymlinkedBasePinnedPublicationScript(t *testing.T) {
	t.Parallel()

	fixture := newPushFixture(t, pushFixtureOptions{trustedValidatorSymlink: true})
	output, exitCode := fixture.run(t)
	require.Equal(t, 1, exitCode, output)
	require.Contains(t, output, "trusted base does not provide readable non-symlink regular file scripts/agent-check-pr")

	remoteHead := runGitOutput(t, fixture.root, "--git-dir", fixture.remote, "rev-parse", "refs/heads/feature")
	require.Equal(t, fixture.headSHA, remoteHead)
}

func TestPushKeepsExecutableRequirementForDirectTrustedTools(t *testing.T) {
	t.Parallel()

	fixture := newPushFixture(t, pushFixtureOptions{trustedDirectToolNonExecutable: true})
	output, exitCode := fixture.run(t)
	require.Equal(t, 1, exitCode, output)
	require.Contains(t, output, "trusted base does not provide executable scripts/ai-review-codex")

	remoteHead := runGitOutput(t, fixture.root, "--git-dir", fixture.remote, "rev-parse", "refs/heads/feature")
	require.Equal(t, fixture.headSHA, remoteHead)
}

func TestPushQuotesValidationPathsContainingAnApostrophe(t *testing.T) {
	t.Parallel()

	// The run directory inherits the repository parent path, so the guarded
	// validation recipe must survive a checkout such as `/tmp/user's repo`.
	fixture := newPushFixture(t, pushFixtureOptions{quotedRepositoryParent: true})
	output, exitCode := fixture.run(t)
	require.Equal(t, 0, exitCode, output)
	require.Contains(t, output, "AI_PR_LOOP_PUSH_RESULT: PUSHED")
}

func TestPushRevalidatesTargetAtEveryPublicationBoundary(t *testing.T) {
	for _, test := range []struct {
		name  string
		stage string
	}{
		{name: "after initial review before readiness", stage: "after-initial-review"},
		{name: "during exact review before authorization", stage: "after-exact-review"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newPushFixture(t, pushFixtureOptions{targetMutation: test.stage})
			output, exitCode := fixture.run(t)
			require.Equal(t, 3, exitCode, output)
			require.Contains(t, output, "BASE_REVALIDATION_CLASSIFICATION=ADVANCED")
			require.Contains(t, output, "AI_PR_LOOP_RESULT: BASE_UPDATE_REQUIRED")
			require.NotContains(t, output, "AI_PR_LOOP_RESULT: READY_FOR_HUMAN_REVIEW")
			require.NotContains(t, output, "AI_PR_LOOP_PUSH_RESULT: PUSHED")
			require.Contains(t, output, "EXPECTED_BASE_SHA="+fixture.baseSHA)
			require.Contains(t, output, "OBSERVED_BASE_SHA="+fixture.advancedBaseSHA)
			require.Contains(t, output, "REQUIRED_NEXT_ACTION=")
			worktree := worktreeFromOutput(t, output)
			require.DirExists(t, worktree, "stale-base work must remain inspectable")
			require.Equal(t, fixture.headSHA, runGitOutput(t, fixture.root, "--git-dir", fixture.remote, "rev-parse", "refs/heads/feature"))
		})
	}
}

func TestPushFailsClosedWhenTargetIsRewrittenMidRun(t *testing.T) {
	fixture := newPushFixture(t, pushFixtureOptions{targetMutation: "rewrite-after-initial-review"})
	output, exitCode := fixture.run(t)
	require.Equal(t, 1, exitCode, output)
	require.Contains(t, output, "BASE_REVALIDATION_CLASSIFICATION=REWRITTEN_OR_DIVERGED")
	require.Contains(t, output, "AI_PR_LOOP_RESULT: ERROR (target base rewritten or diverged)")
	require.NotContains(t, output, "AI_PR_LOOP_RESULT: READY_FOR_HUMAN_REVIEW")
	require.NotContains(t, output, "AI_PR_LOOP_PUSH_RESULT: PUSHED")
}

func TestPushFailsClosedWhenLastMileTargetFetchFails(t *testing.T) {
	fixture := newPushFixture(t, pushFixtureOptions{targetMutation: "fetch-error-after-initial-review"})
	output, exitCode := fixture.run(t)
	require.Equal(t, 1, exitCode, output)
	require.Contains(t, output, "BASE_REVALIDATION_CLASSIFICATION=FETCH_ERROR")
	require.Contains(t, output, "AI_PR_LOOP_RESULT: ERROR (target base fetch failed)")
	require.NotContains(t, output, "AI_PR_LOOP_RESULT: READY_FOR_HUMAN_REVIEW")
	require.NotContains(t, output, "AI_PR_LOOP_PUSH_RESULT: PUSHED")
	require.DirExists(t, worktreeFromOutput(t, output), "candidate work must survive a fetch failure")
}

type pushFixtureOptions struct {
	moveRemote                           bool
	moveLocalHead                        bool
	tamperTargetToolchain                bool
	omitTrustedValidator                 bool
	trustedPublicationToolsNonExecutable bool
	trustedValidatorDirectory            bool
	trustedValidatorSymlink              bool
	trustedDirectToolNonExecutable       bool
	targetMutation                       string

	// quotedRepositoryParent nests the repository under a parent directory whose
	// name contains an apostrophe, which the run directory inherits.
	quotedRepositoryParent bool
}

type pushFixture struct {
	root            string
	remote          string
	checkout        string
	fakeBin         string
	baseSHA         string
	headSHA         string
	advancedBaseSHA string
	divergedBaseSHA string
	reviewCountFile string
	options         pushFixtureOptions
}

func newPushFixture(t *testing.T, options pushFixtureOptions) pushFixture {
	t.Helper()

	root := t.TempDir()
	if options.quotedRepositoryParent {
		root = filepath.Join(root, "ai loop's fixtures")
		require.NoError(t, os.MkdirAll(root, 0o755))
	}
	remote := filepath.Join(root, "remote.git")
	seed := filepath.Join(root, "seed")
	checkout := filepath.Join(root, "checkout")

	runGit(t, root, "init", "--bare", remote)
	require.NoError(t, os.MkdirAll(filepath.Join(seed, "scripts"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(seed, "docs", "technical", "contributing"), 0o755))
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
	bugfixGate, err := os.ReadFile(filepath.Join(filepath.Dir(launcherPath(t)), "ai-bugfix-gate"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "ai-bugfix-gate"), bugfixGate, 0o755))
	publicationToolMode := os.FileMode(0o755)
	if options.trustedPublicationToolsNonExecutable {
		publicationToolMode = 0o644
	}
	if !options.omitTrustedValidator {
		validatorPath := filepath.Join(seed, "scripts", "agent-check-pr")
		switch {
		case options.trustedValidatorDirectory:
			require.NoError(t, os.Mkdir(validatorPath, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(validatorPath, "tracked"), []byte("not a script\n"), 0o644))
		case options.trustedValidatorSymlink:
			externalValidatorPath := filepath.Join(root, "outside-agent-check-pr")
			require.NoError(t, os.WriteFile(externalValidatorPath, []byte("#!/usr/bin/env bash\nexit 0\n"), 0o644))
			require.NoError(t, os.Symlink(externalValidatorPath, validatorPath))
		default:
			require.NoError(t, os.WriteFile(validatorPath, []byte(`#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
    --list) printf 'agent-check\n' ;;
    --normalize) bash "$(dirname "$0")/agent-just" pre-commit ;;
esac
`), publicationToolMode))
		}
		require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "agent-just"), []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "${TEST_TARGET_MUTATION:-}" == "before-exact-review" && ! -e "$TEST_TARGET_MUTATION_MARKER" ]]; then
    git --git-dir="$TEST_REMOTE" update-ref refs/heads/release/v3.0 "$TEST_ADVANCED_BASE_SHA"
    : > "$TEST_TARGET_MUTATION_MARKER"
fi
exit 0
`), publicationToolMode))
	}
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "agent-validation-env"), []byte(`#!/usr/bin/env bash
set -euo pipefail
run_dir=$1
shift
mkdir -p "$run_dir/tmp"
exec "$@"
`), publicationToolMode))
	directToolMode := os.FileMode(0o755)
	if options.trustedDirectToolNonExecutable {
		directToolMode = 0o644
	}
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "ai-review-codex"), []byte("#!/usr/bin/env bash\nexit 0\n"), directToolMode))
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "ai-fix-claude"), []byte("#!/usr/bin/env bash\nexit 0\n"), publicationToolMode))
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "ai-pr-known-findings"), []byte(`#!/usr/bin/env bash
set -euo pipefail
pr=$1
shift
head=""
output=""
while [[ $# -gt 0 ]]; do
	case "$1" in
		--head)
			head=$2
			shift 2
			;;
		--output)
			output=$2
			shift 2
			;;
		*)
			shift
			;;
	esac
done
[[ -n "$head" && -n "$output" ]]
printf '{"version":1,"pr_number":%s,"head":"%s","review_decision":"REVIEW_REQUIRED","findings":[]}\n' "$pr" "$head" > "$output"
`), 0o644))
	writeExecutable(t, filepath.Join(seed, "scripts", "review-loop"), `#!/usr/bin/env bash
set -euo pipefail
review_cmd=""
fix_cmd=""
validation_cmd=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --review-cmd)
            review_cmd=$2
            shift 2
            ;;
        --fix-cmd)
            fix_cmd=$2
            shift 2
            ;;
        --validation-cmd)
            validation_cmd=$2
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done
case "$review_cmd" in
    *trusted-tools/scripts/ai-review-codex) ;;
    *) exit 95 ;;
esac
eval "review_script=${review_cmd#bash }"
trusted_root=$(git -C "$(dirname "$review_script")" rev-parse --show-toplevel)
[[ -n "${AI_REVIEW_KNOWN_FINDINGS:-}" && -f "$AI_REVIEW_KNOWN_FINDINGS" ]] || exit 92
if [[ -n "$fix_cmd" ]]; then
    case "$fix_cmd" in
        *trusted-tools/scripts/ai-fix-claude) ;;
        *) exit 94 ;;
    esac
fi
# The guarded validation recipe nests a program inside bash -c, so that
# program must itself be valid shell input for any repository path.
validation_words=()
eval "validation_words=($validation_cmd)"
validation_program=${validation_words[$((${#validation_words[@]} - 1))]}
bash -n -c "$validation_program" || exit 90
case "$validation_program" in
    *trusted-tools/scripts/agent-check-pr) ;;
    *) exit 93 ;;
esac
count=0
if [[ -f "$TEST_REVIEW_COUNT_FILE" ]]; then
    count=$(cat "$TEST_REVIEW_COUNT_FILE")
fi
count=$((count + 1))
printf '%s' "$count" > "$TEST_REVIEW_COUNT_FILE"
if [[ "$count" -eq 1 ]]; then
    printf 'review fix\n' >> feature.txt
    case "${TEST_TARGET_MUTATION:-}" in
        after-initial-review)
            git --git-dir="$TEST_REMOTE" update-ref refs/heads/release/v3.0 "$TEST_ADVANCED_BASE_SHA"
            ;;
        rewrite-after-initial-review)
            git --git-dir="$TEST_REMOTE" update-ref refs/heads/release/v3.0 "$TEST_DIVERGED_BASE_SHA"
            ;;
        fetch-error-after-initial-review)
            mv "$TEST_REMOTE" "$TEST_REMOTE.unavailable"
            ;;
    esac
elif [[ "$count" -eq 2 && "${TEST_MOVE_REMOTE:-false}" == "true" ]]; then
    git --git-dir="$TEST_REMOTE" update-ref refs/heads/feature "$TEST_BASE_SHA"
elif [[ "$count" -eq 2 && "${TEST_MOVE_LOCAL_HEAD:-false}" == "true" ]]; then
    printf 'unreviewed commit\n' > unreviewed.txt
    git add unreviewed.txt
    git commit -m 'test: move reviewed candidate head'
elif [[ "$count" -eq 2 && "${TEST_TARGET_MUTATION:-}" == "after-exact-review" ]]; then
    git --git-dir="$TEST_REMOTE" update-ref refs/heads/release/v3.0 "$TEST_ADVANCED_BASE_SHA"
fi
exit 0
`)
	require.NoError(t, os.WriteFile(filepath.Join(seed, "docs", "technical", "contributing", "ai-pr-known-findings.md"), []byte("trusted known-findings contract\n"), 0o644))
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
	divergedBaseSHA := createCommitObject(t, remote, baseSHA+"^{tree}", "diverged target fixture")

	runGit(t, seed, "checkout", "-b", "feature")
	require.NoError(t, os.WriteFile(filepath.Join(seed, "feature.txt"), []byte("feature\n"), 0o644))
	if options.tamperTargetToolchain {
		writeExecutable(t, filepath.Join(seed, "scripts", "review-loop"), "#!/usr/bin/env bash\nexit 97\n")
		writeExecutable(t, filepath.Join(seed, "scripts", "ai-review-codex"), "#!/usr/bin/env bash\nexit 97\n")
		writeExecutable(t, filepath.Join(seed, "scripts", "ai-pr-known-findings"), "#!/usr/bin/env bash\nexit 97\n")
		writeExecutable(t, filepath.Join(seed, "scripts", "ai-fix-claude"), "#!/usr/bin/env bash\nexit 97\n")
		writeExecutable(t, filepath.Join(seed, "scripts", "agent-check-pr"), "#!/usr/bin/env bash\nexit 97\n")
	}
	runGit(t, seed, "add", "feature.txt")
	if options.tamperTargetToolchain {
		runGit(t, seed, "add", "scripts/review-loop", "scripts/ai-review-codex", "scripts/ai-pr-known-findings", "scripts/ai-fix-claude", "scripts/agent-check-pr")
	}
	runGit(t, seed, "commit", "-m", "feature")
	headSHA := runGitOutput(t, seed, "rev-parse", "HEAD")
	runGit(t, seed, "push", "-u", "origin", "feature")
	runGit(t, root, "clone", "--branch", "release/v3.0", remote, checkout)
	runGit(t, checkout, "config", "user.name", "AI PR Loop Test")
	runGit(t, checkout, "config", "user.email", "ai-pr-loop@example.com")

	fakeBin := filepath.Join(root, "bin")
	require.NoError(t, os.MkdirAll(fakeBin, 0o755))
	writeExecutable(t, filepath.Join(fakeBin, "gh"), `#!/usr/bin/env bash
set -euo pipefail
case "$1 $2" in
    "repo view")
        printf 'owner/repo\n'
        ;;
    "pr view")
        printf '{"number":123,"url":"https://github.com/owner/repo/pull/123","state":"OPEN","isDraft":false,"title":"chore: tooling","body":"","labels":[],"baseRefName":"release/v3.0","baseRefOid":"%s","headRefName":"feature","headRefOid":"%s","headRepositoryOwner":{"login":"owner"},"headRepository":{"name":"repo"}}\n' "$TEST_BASE_SHA" "$TEST_HEAD_SHA"
        ;;
    *)
        exit 98
        ;;
esac
`)
	writeExecutable(t, filepath.Join(fakeBin, "nix"), `#!/usr/bin/env bash
set -euo pipefail
source_root=""
output=""
if [[ " $* " == *" --command bash -c "* ]]; then
    eval "${@: -1}"
    exit $?
fi
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
if [[ -z "$source_root" || -z "$output" ]]; then
    exit 96
fi
cp "$source_root/scripts/review-loop" "$output"
chmod 755 "$output"
`)

	return pushFixture{
		root:            root,
		remote:          remote,
		checkout:        checkout,
		fakeBin:         fakeBin,
		baseSHA:         baseSHA,
		headSHA:         headSHA,
		advancedBaseSHA: advancedBaseSHA,
		divergedBaseSHA: divergedBaseSHA,
		reviewCountFile: filepath.Join(root, "review-count"),
		options:         options,
	}
}

func (fixture pushFixture) run(t *testing.T) (string, int) {
	t.Helper()

	command := exec.Command("bash", filepath.Join(fixture.checkout, "scripts", "ai-pr-loop"), "123", "--push", "--keep-worktree")
	command.Dir = fixture.checkout
	command.Env = testenv.Environment(
		"PATH="+fixture.fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"TEST_BASE_SHA="+fixture.baseSHA,
		"TEST_HEAD_SHA="+fixture.headSHA,
		"TEST_REVIEW_COUNT_FILE="+fixture.reviewCountFile,
		"TEST_MOVE_REMOTE="+strconv.FormatBool(fixture.options.moveRemote),
		"TEST_MOVE_LOCAL_HEAD="+strconv.FormatBool(fixture.options.moveLocalHead),
		"TEST_REMOTE="+fixture.remote,
		"TEST_ADVANCED_BASE_SHA="+fixture.advancedBaseSHA,
		"TEST_DIVERGED_BASE_SHA="+fixture.divergedBaseSHA,
		"TEST_TARGET_MUTATION="+fixture.options.targetMutation,
		"TEST_TARGET_MUTATION_MARKER="+filepath.Join(fixture.root, "target-mutation-marker"),
	)
	output, err := command.CombinedOutput()
	if err == nil {
		return string(output), 0
	}
	var exitError *exec.ExitError
	require.ErrorAs(t, err, &exitError, string(output))

	return string(output), exitError.ExitCode()
}

func createCommitObject(t *testing.T, gitDirectory, tree, message string) string {
	t.Helper()
	command := testenv.Command(t, "git", "--git-dir", gitDirectory, "commit-tree", tree, "-m", message)
	command.Env = testenv.Environment(
		"GIT_AUTHOR_NAME=Target Rewrite",
		"GIT_AUTHOR_EMAIL=target-rewrite@example.com",
		"GIT_COMMITTER_NAME=Target Rewrite",
		"GIT_COMMITTER_EMAIL=target-rewrite@example.com",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))

	return strings.TrimSpace(string(output))
}
