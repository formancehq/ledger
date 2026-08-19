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

	fixture := newPushFixture(t, false)
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

	fixture := newPushFixture(t, true)
	output, exitCode := fixture.run(t)
	require.Equal(t, 2, exitCode, output)
	require.Contains(t, output, "AI_PR_LOOP_PUSH_RESULT: REFUSED (remote PR head moved)")

	remoteHead := runGitOutput(t, fixture.root, "--git-dir", fixture.remote, "rev-parse", "refs/heads/feature")
	require.Equal(t, fixture.baseSHA, remoteHead)
}

type pushFixture struct {
	root            string
	remote          string
	checkout        string
	fakeBin         string
	baseSHA         string
	headSHA         string
	reviewCountFile string
	moveRemote      bool
}

func newPushFixture(t *testing.T, moveRemote bool) pushFixture {
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
	writeExecutable(t, filepath.Join(seed, "scripts", "review-loop"), `#!/usr/bin/env bash
set -euo pipefail
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
	runGit(t, seed, "add", "feature.txt")
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

	return pushFixture{
		root:            root,
		remote:          remote,
		checkout:        checkout,
		fakeBin:         fakeBin,
		baseSHA:         baseSHA,
		headSHA:         headSHA,
		reviewCountFile: filepath.Join(root, "review-count"),
		moveRemote:      moveRemote,
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
		"TEST_MOVE_REMOTE="+strconv.FormatBool(fixture.moveRemote),
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
