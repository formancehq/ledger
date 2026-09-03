package aiprloop

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoveryDiscoversInterruptedCandidateWithOrdinaryGit(t *testing.T) {
	t.Parallel()

	repository := filepath.Join(t.TempDir(), "repository")
	require.NoError(t, os.MkdirAll(repository, 0o755))
	runGit(t, repository, "init")
	runGit(t, repository, "config", "user.name", "Recovery Test")
	runGit(t, repository, "config", "user.email", "recovery@example.com")
	require.NoError(t, os.WriteFile(filepath.Join(repository, "base.txt"), []byte("base\n"), 0o644))
	runGit(t, repository, "add", "base.txt")
	runGit(t, repository, "commit", "-m", "base")
	baseSHA := runGitOutput(t, repository, "rev-parse", "HEAD")

	runGit(t, repository, "switch", "-c", "interrupted-candidate")
	require.NoError(t, os.WriteFile(filepath.Join(repository, "candidate.txt"), []byte("preserved\n"), 0o644))
	runGit(t, repository, "add", "candidate.txt")
	runGit(t, repository, "commit", "-m", "candidate")
	candidateSHA := runGitOutput(t, repository, "rev-parse", "HEAD")

	inspection := filepath.Join(t.TempDir(), "clean-inspection")
	runGit(t, repository, "worktree", "add", "--detach", inspection, candidateSHA)
	t.Cleanup(func() { runGit(t, repository, "worktree", "remove", inspection) })

	require.Equal(t, candidateSHA, runGitOutput(t, inspection, "rev-parse", "HEAD"))
	require.Equal(t, "candidate.txt", runGitOutput(t, inspection, "diff", "--name-only", baseSHA, candidateSHA, "--"))
	require.Contains(t, runGitOutput(t, inspection, "branch", "--contains", candidateSHA), "interrupted-candidate")
}

func TestRecoveryRequiresFreshReviewAndDoesNotReuseOldReceipt(t *testing.T) {
	t.Parallel()

	fixture := newLauncherFixture(t)
	staleReceipt := filepath.Join(fixture.checkout, "build", "ai-review-loop", "review-1.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(staleReceipt), 0o755))
	require.NoError(t, os.WriteFile(staleReceipt, []byte(`{"decision":"APPROVE"}`), 0o644))

	firstCapture := filepath.Join(fixture.root, "first-recovery-review")
	firstOutput, firstErr := runLauncher(t, fixture, firstCapture)
	require.NoError(t, firstErr, firstOutput)
	secondCapture := filepath.Join(fixture.root, "second-recovery-review")
	secondOutput, secondErr := runLauncher(t, fixture, secondCapture)
	require.NoError(t, secondErr, secondOutput)

	require.FileExists(t, firstCapture, "the first clean invocation must start a review")
	require.FileExists(t, secondCapture, "the resumed clean invocation must start a fresh review")
	require.NotEqual(t, capturedArgument(t, firstCapture, "--state-dir"), capturedArgument(t, secondCapture, "--state-dir"))
	require.Contains(t, capturedArgument(t, firstCapture, "--review-cmd"), "ai-review-codex")
	require.Contains(t, capturedArgument(t, secondCapture, "--review-cmd"), "ai-review-codex")
	require.FileExists(t, staleReceipt, "recovery must neither trust nor rewrite unrelated historical state")
}
