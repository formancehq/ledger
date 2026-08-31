package targetbaserevalidation

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTargetBaseRevalidationClassifications(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name           string
		mutate         func(*testing.T, revalidationFixture)
		expectedCode   int
		classification string
	}{
		{name: "unchanged", expectedCode: 0, classification: "UNCHANGED"},
		{
			name: "advanced",
			mutate: func(t *testing.T, fixture revalidationFixture) {
				writeAndCommit(t, fixture.seed, "advanced.txt", "advanced\n", "advance target")
				runGit(t, fixture.seed, "push", "origin", "release/v3.0")
			},
			expectedCode:   3,
			classification: "ADVANCED",
		},
		{
			name: "rewritten or diverged",
			mutate: func(t *testing.T, fixture revalidationFixture) {
				runGit(t, fixture.seed, "switch", "--orphan", "replacement")
				writeAndCommit(t, fixture.seed, "replacement.txt", "replacement\n", "replace target")
				runGit(t, fixture.seed, "push", "--force", "origin", "HEAD:release/v3.0")
			},
			expectedCode:   4,
			classification: "REWRITTEN_OR_DIVERGED",
		},
		{
			name: "fetch error",
			mutate: func(t *testing.T, fixture revalidationFixture) {
				runGit(t, fixture.checkout, "remote", "set-url", "origin", filepath.Join(fixture.root, "missing.git"))
			},
			expectedCode:   1,
			classification: "FETCH_ERROR",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRevalidationFixture(t)
			if test.mutate != nil {
				test.mutate(t, fixture)
			}
			command := exec.Command("bash", revalidatorPath(t), "origin", "release/v3.0", fixture.baseSHA)
			command.Dir = fixture.checkout
			output, err := command.CombinedOutput()
			exitCode := 0
			if err != nil {
				var exitError *exec.ExitError
				require.ErrorAs(t, err, &exitError, string(output))
				exitCode = exitError.ExitCode()
			}
			require.Equal(t, test.expectedCode, exitCode, string(output))
			require.Contains(t, string(output), "BASE_REVALIDATION_CLASSIFICATION="+test.classification)
			require.Contains(t, string(output), "EXPECTED_BASE_SHA="+fixture.baseSHA)
			if test.classification != "FETCH_ERROR" {
				require.Contains(t, string(output), "OBSERVED_BASE_SHA=")
			}
		})
	}
}

type revalidationFixture struct {
	root     string
	seed     string
	checkout string
	baseSHA  string
}

func newRevalidationFixture(t *testing.T) revalidationFixture {
	t.Helper()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	seed := filepath.Join(root, "seed")
	checkout := filepath.Join(root, "checkout")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", seed)
	runGit(t, seed, "config", "user.name", "Base Revalidation Test")
	runGit(t, seed, "config", "user.email", "base-revalidation@example.com")
	writeAndCommit(t, seed, "base.txt", "base\n", "base")
	runGit(t, seed, "branch", "-M", "release/v3.0")
	baseSHA := runGitOutput(t, seed, "rev-parse", "HEAD")
	runGit(t, seed, "remote", "add", "origin", remote)
	runGit(t, seed, "push", "-u", "origin", "release/v3.0")
	runGit(t, root, "clone", "--branch", "release/v3.0", remote, checkout)

	return revalidationFixture{root: root, seed: seed, checkout: checkout, baseSHA: baseSHA}
}

func writeAndCommit(t *testing.T, repository, name, content, message string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(repository, name), []byte(content), 0o644))
	runGit(t, repository, "add", name)
	runGit(t, repository, "commit", "-m", message)
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

func revalidatorPath(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "ai-target-base-revalidate"))
	require.NoError(t, err)

	return path
}
