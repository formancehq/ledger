package aiprloop

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLauncherReportsBaseUpdateRequiredBeforeTriage(t *testing.T) {
	fixture := newLauncherFixture(t)

	updater := filepath.Join(fixture.root, "updater")
	runGit(t, fixture.root, "clone", "--branch", "release/v3.0", filepath.Join(fixture.root, "remote.git"), updater)
	runGit(t, updater, "config", "user.name", "Target Branch Update")
	runGit(t, updater, "config", "user.email", "target-update@example.com")
	require.NoError(t, os.WriteFile(filepath.Join(updater, "advanced.txt"), []byte("advanced\n"), 0o644))
	runGit(t, updater, "add", "advanced.txt")
	runGit(t, updater, "commit", "-m", "advance target")
	currentBase := runGitOutput(t, updater, "rev-parse", "HEAD")
	runGit(t, updater, "push", "origin", "release/v3.0")

	capture := filepath.Join(fixture.root, "review-args")
	output, err := runLauncher(t, fixture, capture, triageResult{decision: "KEEP"})
	require.Error(t, err, output)
	var exitError *exec.ExitError
	require.ErrorAs(t, err, &exitError)
	require.Equal(t, 3, exitError.ExitCode(), output)
	require.Contains(t, output, "AI_PR_LOOP_RESULT: BASE_UPDATE_REQUIRED")
	require.Contains(t, output, fixture.baseSHA)
	require.Contains(t, output, currentBase)
	require.NoFileExists(t, capture, "technical review must not run")
}

func TestLauncherDeepensShallowCloneBeforeClassifyingBaseAdvance(t *testing.T) {
	fixture := newLauncherFixture(t)

	updater := filepath.Join(fixture.root, "updater")
	runGit(t, fixture.root, "clone", "--branch", "release/v3.0", filepath.Join(fixture.root, "remote.git"), updater)
	runGit(t, updater, "config", "user.name", "Target Branch Update")
	runGit(t, updater, "config", "user.email", "target-update@example.com")
	require.NoError(t, os.WriteFile(filepath.Join(updater, "advanced.txt"), []byte("advanced\n"), 0o644))
	runGit(t, updater, "add", "advanced.txt")
	runGit(t, updater, "commit", "-m", "advance target")
	runGit(t, updater, "push", "origin", "release/v3.0")

	shallowCheckout := filepath.Join(fixture.root, "shallow-checkout")
	remoteURL := "file://" + filepath.Join(fixture.root, "remote.git")
	runGit(t, fixture.root, "clone", "--depth=1", "--branch", "release/v3.0", remoteURL, shallowCheckout)
	require.Equal(t, "true", runGitOutput(t, shallowCheckout, "rev-parse", "--is-shallow-repository"))
	fixture.checkout = shallowCheckout

	capture := filepath.Join(fixture.root, "review-args")
	output, err := runLauncher(t, fixture, capture, triageResult{decision: "KEEP"})
	require.Error(t, err, output)
	var exitError *exec.ExitError
	require.ErrorAs(t, err, &exitError)
	require.Equal(t, 3, exitError.ExitCode(), output)
	require.Contains(t, output, "AI_PR_LOOP_RESULT: BASE_UPDATE_REQUIRED")
	require.Equal(t, "false", runGitOutput(t, shallowCheckout, "rev-parse", "--is-shallow-repository"))
	require.NoFileExists(t, capture, "technical review must not run")
}

func TestLauncherTreatsRewrittenTargetAsError(t *testing.T) {
	fixture := newLauncherFixture(t)

	rewriter := filepath.Join(fixture.root, "rewriter")
	runGit(t, fixture.root, "clone", "--branch", "release/v3.0", filepath.Join(fixture.root, "remote.git"), rewriter)
	runGit(t, rewriter, "config", "user.name", "Target Branch Rewrite")
	runGit(t, rewriter, "config", "user.email", "target-rewrite@example.com")
	runGit(t, rewriter, "checkout", "--orphan", "rewritten")
	require.NoError(t, os.WriteFile(filepath.Join(rewriter, "replacement.txt"), []byte("replacement\n"), 0o644))
	runGit(t, rewriter, "add", "replacement.txt")
	runGit(t, rewriter, "commit", "-m", "rewrite target")
	runGit(t, rewriter, "push", "--force", "origin", "HEAD:release/v3.0")

	capture := filepath.Join(fixture.root, "review-args")
	output, err := runLauncher(t, fixture, capture, triageResult{decision: "KEEP"})
	require.Error(t, err, output)
	var exitError *exec.ExitError
	require.ErrorAs(t, err, &exitError)
	require.Equal(t, 1, exitError.ExitCode(), output)
	require.Contains(t, output, "AI_PR_LOOP_RESULT: ERROR (target base rewritten or diverged)")
	require.NoFileExists(t, capture, "technical review must not run")
}
