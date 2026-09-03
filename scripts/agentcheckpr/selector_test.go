package agentcheckpr

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/scripts/internal/testenv"
)

func TestSelectorEnvironmentDropsParentReviewBinding(t *testing.T) {
	validationRunDirectory := t.TempDir()
	gitGuardDirectory := filepath.Join(validationRunDirectory, "git-guard-bin")
	t.Setenv("VALIDATION_RUN_DIR", validationRunDirectory)
	t.Setenv("PATH", strings.Join([]string{gitGuardDirectory, "/usr/bin"}, string(os.PathListSeparator)))

	binding := map[string]string{
		"EXPECTED_PR_NUMBER":        "1852",
		"EXPECTED_WORKTREE":         "/parent/candidate",
		"EXPECTED_HEAD":             "deadbeef",
		"AI_WORKTREE_PR":            "1852",
		"AI_WORKTREE_PATH":          "/parent/candidate",
		"AI_WORKTREE_EXPECTED_HEAD": "deadbeef",
		"TRUSTED_ROOT_CHECKOUT":     "/parent/root",
	}
	for name, value := range binding {
		t.Setenv(name, value)
	}

	environment := map[string]string{}
	for _, value := range testenv.Environment() {
		name, contents, _ := strings.Cut(value, "=")
		environment[name] = contents
	}

	for name := range binding {
		_, present := environment[name]
		require.False(t, present, "%s must not escape into the synthetic repository", name)
	}
	require.Equal(t, "/usr/bin", environment["PATH"])
}

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

			stdout, _ := runSelectorList(t, repository, testenv.Environment("AI_REVIEW_BASE_SHA="+baseSHA))
			require.Equal(t, testCase.expected, strings.Fields(stdout))
		})
	}
}

func TestSelectorListKeepsDiagnosticsOutOfStructuredStdout(t *testing.T) {
	t.Parallel()

	repository := t.TempDir()
	runGit(t, repository, "init")
	require.NoError(t, os.WriteFile(filepath.Join(repository, "base.txt"), []byte("base\n"), 0o644))
	runGit(t, repository, "add", ".")
	runGit(t, repository, "-c", "user.name=Agent Check PR Test", "-c", "user.email=agent-check-pr@example.com", "commit", "-m", "base")
	baseSHA := runGitOutput(t, repository, "rev-parse", "HEAD")
	writeTestFile(t, repository, "misc/operator/internal/example.go")
	runGit(t, repository, "add", ".")
	runGit(t, repository, "-c", "user.name=Agent Check PR Test", "-c", "user.email=agent-check-pr@example.com", "commit", "-m", "changes")

	runDirectory := t.TempDir()
	isolatedDirectories := map[string]string{
		"HOME":                "home",
		"GOCACHE":             "go-cache",
		"GOMODCACHE":          "go-mod-cache",
		"GOPATH":              "go-path",
		"TMPDIR":              "tmp",
		"XDG_CACHE_HOME":      "cache",
		"GOLANGCI_LINT_CACHE": "cache/golangci-lint",
	}
	environment := testenv.Environment(
		"AI_REVIEW_BASE_SHA="+baseSHA,
		"VALIDATION_RUN_DIR="+runDirectory,
		"VALIDATION_RUN_ID=selector-test",
	)
	for name, relativePath := range isolatedDirectories {
		path := filepath.Join(runDirectory, relativePath)
		require.NoError(t, os.MkdirAll(path, 0o755))
		environment = append(environment, name+"="+path)
	}

	stdout, stderr := runSelectorList(t, repository, environment)
	require.Equal(t, "agent-check-full\ntest-operator\n", stdout, "stdout must contain structured gates only")
	require.Contains(t, stderr, "LINT_ISOLATION_GATE=PASS (", "stderr may contain validation diagnostics")
}

func runSelectorList(t *testing.T, repository string, environment []string) (string, string) {
	t.Helper()

	command := exec.Command("bash", selectorPath(t), "--list")
	command.Dir = repository
	command.Env = environment
	var stderr bytes.Buffer
	command.Stderr = &stderr

	// --list stdout is a machine-readable protocol; stderr is reserved for
	// diagnostics. Never combine the streams before parsing stdout.
	output, err := command.Output()
	require.NoError(t, err, stderr.String())

	return string(output), stderr.String()
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

	command := testenv.Command(t, "git", arguments...)
	command.Dir = directory
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	require.NoError(t, err, fmt.Sprintf("git %s:\n%s", strings.Join(arguments, " "), stderr.String()))

	return strings.TrimSpace(string(output))
}
