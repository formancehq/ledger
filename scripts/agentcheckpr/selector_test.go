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
	t.Setenv("VALIDATION_RUN_DIR", validationRunDirectory)

	binding := map[string]string{
		"EXPECTED_PR_NUMBER": "1852",
		"EXPECTED_WORKTREE":  "/parent/candidate",
		"EXPECTED_HEAD":      "deadbeef",
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
			expected: []string{"agent-check"},
		},
		{
			name:           "uncommitted HTTP contract",
			paths:          []string{"openapi.yml", "internal/adapter/http/example.go"},
			leaveUntracked: true,
			expected:       []string{"pre-commit", "agent-check", "test-race:./internal/adapter/http", "test-schemathesis"},
		},
		{
			name:     "production and scenario behavior",
			paths:    []string{"internal/domain/example.go", "tests/scenarios/example_test.go"},
			expected: []string{"agent-check-full", "test-scenarios"},
		},
		{
			name:     "FSM and cluster persistence",
			paths:    []string{"internal/infra/state/example.go", "internal/storage/dal/example.go"},
			expected: []string{"pre-commit", "agent-check", "test-race:./internal/infra/state", "test-race:./internal/storage/dal"},
		},
		{
			name:     "restore protocol",
			paths:    []string{"misc/proto/restore.proto"},
			expected: []string{"agent-check-full", "test-e2e", "protocol-revision", "proto-lint"},
		},
		{
			name: "buf plugin",
			// Regression: tools/* are separate modules, so their Go files must
			// not be race-tested as root-module packages.
			paths:    []string{"tools/buf-plugin-ledger/main.go"},
			expected: []string{"pre-commit", "agent-check", "test-tooling", "proto-lint"},
		},
		{
			name:     "protocol revision only",
			paths:    []string{"pkg/grpcprotocol/protocol.go"},
			expected: []string{"agent-check-full", "protocol-revision"},
		},
		{
			// Regression for a classify_path branch that ended with a bare
			// `return` right after `[[ ... ]] && needs_e2e=true`: a
			// dependency-only path (not under misc/proto or internal/proto)
			// makes that conditional's left side false, so the bare return
			// propagated exit status 1 and set -e silently aborted the whole
			// script. go.mod exercises exactly that false branch.
			name:     "dependency-only toolchain change",
			paths:    []string{"go.mod"},
			expected: []string{"agent-check-full"},
		},
		{
			name:     "operator module",
			paths:    []string{"misc/operator/internal/example.go"},
			expected: []string{"pre-commit", "agent-check", "test-operator"},
		},
		{
			// Top-level Antithesis files are harness configuration, not a Go
			// package. Selecting their directory for go test fails with "no Go
			// files" before the actual workload validation can run.
			name:     "antithesis harness documentation",
			paths:    []string{"tests/antithesis/README.md"},
			expected: []string{"pre-commit", "agent-check"},
		},
		{
			name:      "production file renamed outside gated paths",
			basePaths: []string{"internal/domain/example.go"},
			renames: [][2]string{
				{"internal/domain/example.go", "docs/example.md"},
			},
			expected: []string{"agent-check-full"},
		},
		{
			name:     "focused test only",
			paths:    []string{"internal/infra/cache/cache_test.go"},
			expected: []string{"pre-commit", "agent-check", "test-race:./internal/infra/cache"},
		},
		{
			name:     "two files in one package",
			paths:    []string{"internal/infra/cache/a.go", "internal/infra/cache/b.go"},
			expected: []string{"pre-commit", "agent-check", "test-race:./internal/infra/cache"},
		},
		{
			name:     "AI tooling",
			paths:    []string{"scripts/agent-validation-env"},
			expected: []string{"pre-commit", "agent-check", "test-tooling"},
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

			for _, interpreter := range selectorInterpreters(t) {
				stdout, _ := runSelectorListWith(t, interpreter, repository, testenv.Environment("AI_REVIEW_BASE_SHA="+baseSHA))
				require.Equal(t, testCase.expected, strings.Fields(stdout), "interpreter %s", interpreter)
			}
		})
	}
}

// legacyBashCandidates are system shells that may predate bash 4.4. macOS
// ships bash 3.2 as /bin/bash, and `bash scripts/agent-check-pr` resolves to it
// outside the Nix shell: gate selection runs before the Nix re-exec, so it
// must work there too.
var legacyBashCandidates = []string{"/bin/bash"}

// selectorInterpreters returns the PATH bash plus any available bash older than
// 4.4. Before 4.4, expanding an empty array under set -u is an unbound-variable
// error, and BASH_COMPAT does not reproduce that on newer shells, so the
// regression is only observable with a real legacy interpreter.
func selectorInterpreters(t *testing.T) []string {
	t.Helper()

	interpreters := []string{"bash"}
	for _, candidate := range legacyBashCandidates {
		output, err := exec.Command(candidate, "-c", `echo "${BASH_VERSINFO[0]} ${BASH_VERSINFO[1]}"`).Output()
		if err != nil {
			continue
		}

		var major, minor int
		if _, err := fmt.Sscanf(string(output), "%d %d", &major, &minor); err != nil {
			continue
		}
		if major < 4 || (major == 4 && minor < 4) {
			interpreters = append(interpreters, candidate)
		}
	}

	return interpreters
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

	stdout, stderr := runSelectorList(t, repository, testenv.Environment("AI_REVIEW_BASE_SHA="+baseSHA))
	require.Equal(t, "pre-commit\nagent-check\ntest-operator\n", stdout, "stdout must contain structured gates only")
	require.Empty(t, stderr)
}

func runSelectorList(t *testing.T, repository string, environment []string) (string, string) {
	t.Helper()

	return runSelectorListWith(t, "bash", repository, environment)
}

func runSelectorListWith(t *testing.T, interpreter, repository string, environment []string) (string, string) {
	t.Helper()

	command := exec.Command(interpreter, selectorPath(t), "--list")
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
