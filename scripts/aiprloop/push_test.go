package aiprloop

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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

func TestPushAcceptsNonExecutableBasePinnedScripts(t *testing.T) {
	t.Parallel()

	fixture := newPushFixture(t, pushFixtureOptions{trustedToolsNonExecutable: true})
	output, exitCode := fixture.run(t)
	require.Equal(t, 0, exitCode, output)
	require.Contains(t, output, "AI_PR_LOOP_PUSH_RESULT: PUSHED")
}

func TestPushRefusesBaseWithoutTrustedValidator(t *testing.T) {
	t.Parallel()

	fixture := newPushFixture(t, pushFixtureOptions{omitTrustedValidator: true})
	output, exitCode := fixture.run(t)
	require.Equal(t, 1, exitCode, output)
	require.Contains(t, output, "trusted base does not provide scripts/agent-check-pr")

	remoteHead := runGitOutput(t, fixture.root, "--git-dir", fixture.remote, "rev-parse", "refs/heads/feature")
	require.Equal(t, fixture.headSHA, remoteHead)
}

type pushFixtureOptions struct {
	moveRemote                bool
	moveLocalHead             bool
	tamperTargetToolchain     bool
	omitTrustedValidator      bool
	trustedToolsNonExecutable bool
}

type pushFixture struct {
	root            string
	remote          string
	checkout        string
	fakeBin         string
	baseSHA         string
	headSHA         string
	reviewCountFile string
	options         pushFixtureOptions
}

func newPushFixture(t *testing.T, options pushFixtureOptions) pushFixture {
	t.Helper()

	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	seed := filepath.Join(root, "seed")
	checkout := filepath.Join(root, "checkout")

	runGit(t, root, "init", "--bare", remote)
	require.NoError(t, os.MkdirAll(filepath.Join(seed, "scripts"), 0o755))
	runGit(t, seed, "init")
	runGit(t, seed, "config", "user.name", "AI PR Loop Test")
	runGit(t, seed, "config", "user.email", "ai-pr-loop@example.com")

	launcher, err := os.ReadFile(launcherPath(t))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "ai-pr-loop"), launcher, 0o755))
	trustedToolMode := os.FileMode(0o755)
	if options.trustedToolsNonExecutable {
		trustedToolMode = 0o644
	}
	if !options.omitTrustedValidator {
		validator, err := os.ReadFile(filepath.Join(filepath.Dir(launcherPath(t)), "agent-check-pr"))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "agent-check-pr"), validator, trustedToolMode))
	}
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "ai-review-codex"), []byte("#!/usr/bin/env bash\nexit 0\n"), trustedToolMode))
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "ai-fix-claude"), []byte("#!/usr/bin/env bash\nexit 0\n"), trustedToolMode))
	require.NoError(t, os.WriteFile(filepath.Join(seed, "scripts", "ai-pr-triage"), []byte(`#!/usr/bin/env bash
set -euo pipefail
output=""
while [[ $# -gt 0 ]]; do
	case "$1" in
		--output)
			output=$2
			shift 2
			;;
		*)
			shift
			;;
	esac
done
[[ -n "$output" ]]
printf '{"decision":"KEEP","base_sha":"%s","head":"%s"}\n' \
	"$AI_PR_TRIAGE_EXPECT_BASE_SHA" "$AI_PR_TRIAGE_EXPECT_HEAD_SHA" > "$output"
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
if [[ -n "$fix_cmd" ]]; then
    case "$fix_cmd" in
        *trusted-tools/scripts/ai-fix-claude) ;;
        *) exit 94 ;;
    esac
fi
case "$validation_cmd" in
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
elif [[ "$count" -eq 2 && "${TEST_MOVE_REMOTE:-false}" == "true" ]]; then
    git --git-dir="$TEST_REMOTE" update-ref refs/heads/feature "$TEST_BASE_SHA"
elif [[ "$count" -eq 2 && "${TEST_MOVE_LOCAL_HEAD:-false}" == "true" ]]; then
    printf 'unreviewed commit\n' > unreviewed.txt
    git add unreviewed.txt
    git commit -m 'test: move reviewed candidate head'
fi
exit 0
`)
	require.NoError(t, os.WriteFile(filepath.Join(seed, "base.txt"), []byte("base\n"), 0o644))
	runGit(t, seed, "add", ".")
	runGit(t, seed, "commit", "-m", "base")
	runGit(t, seed, "branch", "-M", "release/v3.0")
	baseSHA := runGitOutput(t, seed, "rev-parse", "HEAD")
	runGit(t, seed, "remote", "add", "origin", remote)
	runGit(t, seed, "push", "-u", "origin", "release/v3.0")

	runGit(t, seed, "checkout", "-b", "feature")
	require.NoError(t, os.WriteFile(filepath.Join(seed, "feature.txt"), []byte("feature\n"), 0o644))
	if options.tamperTargetToolchain {
		writeExecutable(t, filepath.Join(seed, "scripts", "review-loop"), "#!/usr/bin/env bash\nexit 97\n")
		writeExecutable(t, filepath.Join(seed, "scripts", "ai-review-codex"), "#!/usr/bin/env bash\nexit 97\n")
		writeExecutable(t, filepath.Join(seed, "scripts", "ai-fix-claude"), "#!/usr/bin/env bash\nexit 97\n")
		writeExecutable(t, filepath.Join(seed, "scripts", "agent-check-pr"), "#!/usr/bin/env bash\nexit 97\n")
	}
	runGit(t, seed, "add", "feature.txt")
	if options.tamperTargetToolchain {
		runGit(t, seed, "add", "scripts/review-loop", "scripts/ai-review-codex", "scripts/ai-fix-claude", "scripts/agent-check-pr")
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
        printf '{"number":123,"url":"https://github.com/owner/repo/pull/123","state":"OPEN","isDraft":false,"baseRefName":"release/v3.0","baseRefOid":"%s","headRefName":"feature","headRefOid":"%s","headRepositoryOwner":{"login":"owner"},"headRepository":{"name":"repo"}}\n' "$TEST_BASE_SHA" "$TEST_HEAD_SHA"
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
		reviewCountFile: filepath.Join(root, "review-count"),
		options:         options,
	}
}

func (fixture pushFixture) run(t *testing.T) (string, int) {
	t.Helper()

	command := exec.Command("bash", filepath.Join(fixture.checkout, "scripts", "ai-pr-loop"), "123", "--push", "--keep-worktree")
	command.Dir = fixture.checkout
	command.Env = append(os.Environ(),
		"PATH="+fixture.fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"TEST_BASE_SHA="+fixture.baseSHA,
		"TEST_HEAD_SHA="+fixture.headSHA,
		"TEST_REVIEW_COUNT_FILE="+fixture.reviewCountFile,
		"TEST_MOVE_REMOTE="+strconv.FormatBool(fixture.options.moveRemote),
		"TEST_MOVE_LOCAL_HEAD="+strconv.FormatBool(fixture.options.moveLocalHead),
		"TEST_REMOTE="+fixture.remote,
	)
	output, err := command.CombinedOutput()
	if err == nil {
		return string(output), 0
	}
	var exitError *exec.ExitError
	require.ErrorAs(t, err, &exitError, string(output))

	return string(output), exitError.ExitCode()
}
