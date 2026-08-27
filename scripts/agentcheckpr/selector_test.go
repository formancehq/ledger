package agentcheckpr

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelectsLocalValidationGatesFromCompleteDiff(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		basePaths      []string
		paths          []string
		renames        [][2]string
		leaveUntracked bool
		expected       []string
	}{
		{
			name:     "documentation only",
			paths:    []string{"docs/example.md"},
			expected: []string{"agent-check-full"},
		},
		{
			name:           "uncommitted HTTP contract",
			paths:          []string{"openapi.yml", "internal/adapter/http/example.go"},
			leaveUntracked: true,
			expected:       []string{"agent-check-full", "test-e2e", "test-schemathesis"},
		},
		{
			name:     "production and scenario behavior",
			paths:    []string{"internal/domain/example.go", "tests/scenarios/example_test.go"},
			expected: []string{"agent-check-full", "test-e2e", "test-scenarios"},
		},
		{
			name:     "FSM and cluster persistence",
			paths:    []string{"internal/infra/state/example.go", "internal/storage/dal/example.go"},
			expected: []string{"agent-check-full", "test-e2e", "test-model-cluster"},
		},
		{
			name:     "restore protocol",
			paths:    []string{"misc/proto/restore.proto"},
			expected: []string{"agent-check-full", "test-e2e", "test-model-cluster"},
		},
		{
			name:     "operator module",
			paths:    []string{"misc/operator/internal/example.go"},
			expected: []string{"agent-check-full", "test-operator"},
		},
		{
			name:      "production file renamed outside gated paths",
			basePaths: []string{"internal/domain/example.go"},
			renames: [][2]string{
				{"internal/domain/example.go", "docs/example.md"},
			},
			expected: []string{"agent-check-full", "test-e2e"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			repository := t.TempDir()
			runGit(t, repository, "init")
			require.NoError(t, os.WriteFile(filepath.Join(repository, "base.txt"), []byte("base\n"), 0o644))
			for _, path := range testCase.basePaths {
				writeTestFile(t, repository, path)
			}
			runGit(t, repository, "add", ".")
			runGit(t, repository, "-c", "user.name=Agent Check PR Test", "-c", "user.email=agent-check-pr@example.com", "commit", "-m", "base")
			baseSHA := runGitOutput(t, repository, "rev-parse", "HEAD")

			for _, path := range testCase.paths {
				writeTestFile(t, repository, path)
			}
			for _, rename := range testCase.renames {
				destination := filepath.Join(repository, filepath.FromSlash(rename[1]))
				require.NoError(t, os.MkdirAll(filepath.Dir(destination), 0o755))
				runGit(t, repository, "mv", "--", rename[0], rename[1])
			}
			if !testCase.leaveUntracked {
				runGit(t, repository, "add", ".")
				runGit(t, repository, "-c", "user.name=Agent Check PR Test", "-c", "user.email=agent-check-pr@example.com", "commit", "-m", "changes")
			}

			command := exec.Command("bash", selectorPath(t), "--list")
			command.Dir = repository
			command.Env = append(os.Environ(), "AI_REVIEW_BASE_SHA="+baseSHA)
			output, err := command.CombinedOutput()
			require.NoError(t, err, string(output))
			require.Equal(t, testCase.expected, strings.Fields(string(output)))
		})
	}
}

func writeTestFile(t *testing.T, repository, path string) {
	t.Helper()

	absolutePath := filepath.Join(repository, filepath.FromSlash(path))
	require.NoError(t, os.MkdirAll(filepath.Dir(absolutePath), 0o755))
	require.NoError(t, os.WriteFile(absolutePath, []byte("change\n"), 0o644))
}

func selectorPath(t *testing.T) string {
	t.Helper()

	path, err := filepath.Abs(filepath.Join("..", "agent-check-pr"))
	require.NoError(t, err)

	return path
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
