package agentvalidationenv

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSharedCacheEnvironment(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	repository := filepath.Join(root, "candidate")
	cacheRoot := filepath.Join(root, "shared-cache")
	userHome := filepath.Join(root, "home")
	require.NoError(t, os.MkdirAll(repository, 0o755))
	require.NoError(t, os.MkdirAll(userHome, 0o755))
	runGit(t, repository, "init")

	first := captureEnvironment(t, repository, cacheRoot, userHome, filepath.Join(root, "run-one"))
	second := captureEnvironment(t, repository, cacheRoot, userHome, filepath.Join(root, "run-two"))

	require.Equal(t, userHome, first["HOME"])
	require.Equal(t, first["GOCACHE"], second["GOCACHE"])
	require.Equal(t, first["GOMODCACHE"], second["GOMODCACHE"])
	require.Equal(t, first["GOPATH"], second["GOPATH"])
	require.Equal(t, first["XDG_CACHE_HOME"], second["XDG_CACHE_HOME"])
	require.Equal(t, first["GOLANGCI_LINT_CACHE"], second["GOLANGCI_LINT_CACHE"])
	require.NotEqual(t, first["TMPDIR"], second["TMPDIR"])
	resolvedCacheRoot, err := filepath.EvalSymlinks(cacheRoot)
	require.NoError(t, err)
	require.Equal(t, resolvedCacheRoot, first["LEDGER_AI_CACHE_ROOT"])
}

func TestRejectsCacheInsideCandidate(t *testing.T) {
	t.Parallel()

	repository := t.TempDir()
	runGit(t, repository, "init")
	command := exec.Command("bash", validationEnvPath(t), t.TempDir(), "true")
	command.Dir = repository
	command.Env = append(os.Environ(), "LEDGER_AI_CACHE_ROOT="+filepath.Join(repository, ".cache"))
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "shared cache")
}

func TestCleanCacheAndRetry(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	repository := filepath.Join(root, "candidate")
	cacheRoot := filepath.Join(root, "shared-cache")
	runDirectory := filepath.Join(root, "run")
	require.NoError(t, os.MkdirAll(repository, 0o755))
	runGit(t, repository, "init")
	captureEnvironment(t, repository, cacheRoot, root, runDirectory)
	sentinel := filepath.Join(cacheRoot, "go-build", "stale")
	require.NoError(t, os.WriteFile(sentinel, []byte("stale"), 0o644))

	command := exec.Command("bash", validationEnvPath(t), "--clean-cache", runDirectory, "true")
	command.Dir = repository
	command.Env = append(os.Environ(), "HOME="+root, "LEDGER_AI_CACHE_ROOT="+cacheRoot)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	_, err = os.Stat(sentinel)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.FileExists(t, filepath.Join(cacheRoot, ".ledger-ai-cache"))
	require.DirExists(t, filepath.Join(cacheRoot, "go-build"))
}

func TestEphemeralRunDirectoryIsRemoved(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	repository := filepath.Join(root, "candidate")
	cacheRoot := filepath.Join(root, "shared-cache")
	temporaryRoot := filepath.Join(root, "temporary")
	require.NoError(t, os.MkdirAll(repository, 0o755))
	require.NoError(t, os.MkdirAll(temporaryRoot, 0o755))
	runGit(t, repository, "init")

	command := exec.Command(
		"bash", validationEnvPath(t), "--ephemeral", "sh", "-c", "printf %s \"$VALIDATION_RUN_DIR\"",
	)
	command.Dir = repository
	command.Env = append(os.Environ(),
		"HOME="+root,
		"LEDGER_AI_CACHE_ROOT="+cacheRoot,
		"TMPDIR="+temporaryRoot,
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	runDirectory := string(output)
	require.NotEmpty(t, runDirectory)
	require.ErrorIs(t, statError(runDirectory), os.ErrNotExist)
	require.DirExists(t, cacheRoot)
}

func TestGoCacheCooperativeScenarios(t *testing.T) {
	root := t.TempDir()
	cacheRoot := filepath.Join(root, "cache")
	first := filepath.Join(root, "first")
	second := filepath.Join(root, "second")
	writeModule(t, first)
	writeModule(t, second)

	runGoTest(t, first, cacheRoot, "1")
	runGoTest(t, second, cacheRoot, "1")

	require.NoError(t, os.WriteFile(filepath.Join(second, "value.go"), []byte("//go:build !special\n\npackage sample\n\nfunc Value() int { return 2 }\n"), 0o644))
	output, err := goTest(second, cacheRoot, "1", false, "")
	require.Error(t, err, output)
	runGoTest(t, second, cacheRoot, "2")

	output, err = goTest(first, cacheRoot, "1", true, "special")
	require.Error(t, err, output)
	output, err = goTest(first, cacheRoot, "2", true, "special")
	require.NoError(t, err, output)

	firstCommand := goTestCommand(first, cacheRoot, "1", true, "")
	secondCommand := goTestCommand(second, cacheRoot, "2", false, "")
	require.NoError(t, firstCommand.Start())
	require.NoError(t, secondCommand.Start())
	require.NoError(t, firstCommand.Wait())
	require.NoError(t, secondCommand.Wait())
}

func captureEnvironment(t *testing.T, repository, cacheRoot, userHome, runDirectory string) map[string]string {
	t.Helper()
	command := exec.Command(
		"bash", validationEnvPath(t), runDirectory, "sh", "-c",
		"env | grep -E '^(HOME|GOCACHE|GOMODCACHE|GOPATH|TMPDIR|XDG_CACHE_HOME|GOLANGCI_LINT_CACHE|LEDGER_AI_CACHE_ROOT)='",
	)
	command.Dir = repository
	command.Env = append(os.Environ(), "HOME="+userHome, "LEDGER_AI_CACHE_ROOT="+cacheRoot)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))

	result := map[string]string{}
	for line := range strings.SplitSeq(strings.TrimSpace(string(output)), "\n") {
		name, value, found := strings.Cut(line, "=")
		require.True(t, found)
		result[name] = value
	}

	return result
}

func writeModule(t *testing.T, directory string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(directory, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "go.mod"), []byte("module example.test/sample\n\ngo 1.24\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "value.go"), []byte("//go:build !special\n\npackage sample\n\nfunc Value() int { return 1 }\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "value_special.go"), []byte("//go:build special\n\npackage sample\n\nfunc Value() int { return 2 }\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "value_test.go"), []byte(`package sample

import (
	"os"
	"strconv"
	"testing"
)

func TestValue(t *testing.T) {
	expected, err := strconv.Atoi(os.Getenv("EXPECTED_VALUE"))
	if err != nil { t.Fatal(err) }
	if Value() != expected { t.Fatalf("got %d, want %d", Value(), expected) }
}
`), 0o644))
}

func runGoTest(t *testing.T, directory, cacheRoot, expected string) {
	t.Helper()
	output, err := goTest(directory, cacheRoot, expected, false, "")
	require.NoError(t, err, output)
}

func goTest(directory, cacheRoot, expected string, race bool, tags string) (string, error) {
	command := goTestCommand(directory, cacheRoot, expected, race, tags)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	err := command.Run()

	return output.String(), err
}

func goTestCommand(directory, cacheRoot, expected string, race bool, tags string) *exec.Cmd {
	arguments := []string{"test", "-count=1"}
	if race {
		arguments = append(arguments, "-race")
	}
	if tags != "" {
		arguments = append(arguments, "-tags", tags)
	}
	arguments = append(arguments, "./...")
	command := exec.Command("go", arguments...)
	command.Dir = directory
	command.Env = append(os.Environ(),
		"EXPECTED_VALUE="+expected,
		"GOCACHE="+filepath.Join(cacheRoot, "go-build"),
		"GOMODCACHE="+filepath.Join(cacheRoot, "go-mod"),
		"GOPATH="+filepath.Join(cacheRoot, "go-path"),
	)

	return command
}

func validationEnvPath(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "agent-validation-env"))
	require.NoError(t, err)

	return path
}

func statError(path string) error {
	_, err := os.Stat(path)

	return err
}

func runGit(t *testing.T, directory string, arguments ...string) {
	t.Helper()
	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, fmt.Sprintf("git %s:\n%s", strings.Join(arguments, " "), output))
}
