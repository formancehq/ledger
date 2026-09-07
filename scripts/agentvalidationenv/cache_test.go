package agentvalidationenv

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/scripts/internal/testenv"
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
	otherRepository := filepath.Join(root, "other-candidate")
	require.NoError(t, os.MkdirAll(otherRepository, 0o755))
	runGit(t, otherRepository, "init")
	other := captureEnvironment(t, otherRepository, cacheRoot, userHome, filepath.Join(root, "run-three"))

	require.Equal(t, userHome, first["HOME"])
	require.Equal(t, first["GOCACHE"], second["GOCACHE"])
	require.Equal(t, first["GOMODCACHE"], second["GOMODCACHE"])
	require.Equal(t, first["GOPATH"], second["GOPATH"])
	require.Equal(t, first["XDG_CACHE_HOME"], second["XDG_CACHE_HOME"])
	require.Equal(t, first["GOLANGCI_LINT_CACHE"], second["GOLANGCI_LINT_CACHE"])
	require.Equal(t, first["GOCACHE"], other["GOCACHE"])
	require.Equal(t, first["GOMODCACHE"], other["GOMODCACHE"])
	require.Equal(t, first["GOPATH"], other["GOPATH"])
	require.Equal(t, first["XDG_CACHE_HOME"], other["XDG_CACHE_HOME"])
	require.Equal(t, first["GOLANGCI_LINT_CACHE"], other["GOLANGCI_LINT_CACHE"])
	require.NotEqual(t, first["TMPDIR"], second["TMPDIR"])
	resolvedCacheRoot := resolvedPath(t, cacheRoot)
	require.Equal(t, filepath.Join(resolvedCacheRoot, "golangci-lint"), first["GOLANGCI_LINT_CACHE"])
	require.Equal(t, resolvedCacheRoot, first["LEDGER_AI_CACHE_ROOT"])
}

func TestRejectsCacheInsideCandidate(t *testing.T) {
	t.Parallel()

	repository := t.TempDir()
	runGit(t, repository, "init")
	command := testenv.Command(t, "bash", validationEnvPath(t), t.TempDir(), "true")
	command.Dir = repository
	command.Env = testenv.Environment("LEDGER_AI_CACHE_ROOT=" + filepath.Join(repository, ".cache"))
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
	environment := captureEnvironment(t, repository, cacheRoot, root, runDirectory)
	goSentinel := filepath.Join(cacheRoot, "go-build", "keep")
	lintSentinel := filepath.Join(environment["GOLANGCI_LINT_CACHE"], "remove")
	moduleSentinel := filepath.Join(cacheRoot, "go-mod", "keep")
	pathSentinel := filepath.Join(cacheRoot, "go-path", "keep")
	xdgSentinel := filepath.Join(cacheRoot, "xdg", "keep")
	for _, path := range []string{goSentinel, lintSentinel, moduleSentinel, pathSentinel, xdgSentinel} {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte("sentinel"), 0o644))
	}

	command := testenv.Command(t, "bash", validationEnvPath(t), "--clean-cache", runDirectory, "true")
	command.Dir = repository
	command.Env = testenv.Environment("HOME="+root, "LEDGER_AI_CACHE_ROOT="+cacheRoot)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.NoFileExists(t, lintSentinel)
	require.FileExists(t, goSentinel)
	require.FileExists(t, moduleSentinel)
	require.FileExists(t, pathSentinel)
	require.FileExists(t, xdgSentinel)
	require.FileExists(t, filepath.Join(cacheRoot, ".ledger-ai-cache"))
	require.DirExists(t, filepath.Join(cacheRoot, "go-build"))
	require.DirExists(t, environment["GOLANGCI_LINT_CACHE"])
}

func TestLintCacheDoesNotLeakPathsAcrossWorktrees(t *testing.T) {
	t.Parallel()

	seed, producer, consumer, cacheRoot := newLintWorktrees(t)
	producerOutput := runLint(t, producer, cacheRoot, filepath.Join(filepath.Dir(seed), "run-producer"))
	require.Contains(t, producerOutput, "x.go:4:2: ineffectual assignment")
	require.Contains(t, producerOutput, "analyzing 1/1 packages")

	producerRoot := resolvedPath(t, producer)
	runGit(t, seed, "worktree", "remove", "--force", producer)
	require.NoDirExists(t, producer)

	consumerOutput := runLint(t, consumer, cacheRoot, filepath.Join(filepath.Dir(seed), "run-consumer"))
	require.Contains(t, consumerOutput, "x.go:4:2: ineffectual assignment")
	require.Contains(t, consumerOutput, "Loaded 1 issues from cache")
	require.Contains(t, consumerOutput, "analyzing 0/1 packages")
	require.Equal(t, diagnosticLines(producerOutput), diagnosticLines(consumerOutput))
	require.NotContains(t, consumerOutput, producerRoot)
	require.NotContains(t, consumerOutput, "../producer/x.go")
	require.NotContains(t, consumerOutput, "no such file or directory")
}

func TestLintCacheWarmReuseAndContentInvalidation(t *testing.T) {
	t.Parallel()

	seed, _, consumer, cacheRoot := newLintWorktrees(t)
	firstOutput := runLint(t, consumer, cacheRoot, filepath.Join(filepath.Dir(seed), "run-first"))
	require.Contains(t, firstOutput, "analyzing 1/1 packages")

	secondOutput := runLint(t, consumer, cacheRoot, filepath.Join(filepath.Dir(seed), "run-second"))
	require.Contains(t, secondOutput, "Loaded 1 issues from cache")
	require.Contains(t, secondOutput, "analyzing 0/1 packages")

	require.NoError(t, os.WriteFile(filepath.Join(consumer, "x.go"), []byte("package cacheleak\n\nfunc Value() int {\n\treturn 2\n}\n"), 0o644))
	modifiedOutput := runLint(t, consumer, cacheRoot, filepath.Join(filepath.Dir(seed), "run-modified"))
	require.Contains(t, modifiedOutput, "analyzing 1/1 packages")
	require.Contains(t, modifiedOutput, "0 issues")
	require.NotContains(t, modifiedOutput, "ineffectual assignment")
}

func TestLintCacheFixesOnlyConsumerWorktree(t *testing.T) {
	t.Parallel()

	seed, producer, consumer, cacheRoot := newLintWorktrees(t)
	unfixed := []byte("package cacheleak\n\nconst Value = \"teh\"\n")
	configuration := []byte("version: \"2\"\nlinters:\n  default: none\n  enable: [misspell]\n")
	for _, worktree := range []string{producer, consumer} {
		require.NoError(t, os.WriteFile(filepath.Join(worktree, "x.go"), unfixed, 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(worktree, ".golangci.yml"), configuration, 0o644))
	}

	producerOutput := runLint(t, producer, cacheRoot, filepath.Join(filepath.Dir(seed), "run-producer"))
	require.Contains(t, producerOutput, "`teh` is a misspelling of `the`")

	producerRoot := resolvedPath(t, producer)
	runGit(t, seed, "worktree", "remove", "--force", producer)
	consumerOutput := runLint(t, consumer, cacheRoot, filepath.Join(filepath.Dir(seed), "run-consumer"), "--fix")
	require.Contains(t, consumerOutput, "analyzing 0/1 packages")
	require.NotContains(t, consumerOutput, producerRoot)
	require.NotContains(t, consumerOutput, "no such file or directory")
	fixed, err := os.ReadFile(filepath.Join(consumer, "x.go"))
	require.NoError(t, err)
	require.Equal(t, "package cacheleak\n\nconst Value = \"the\"\n", string(fixed))
}

func TestConcurrentLintRunsSafelyShareCacheAcrossWorktrees(t *testing.T) {
	t.Parallel()

	seed, first, second, cacheRoot := newLintWorktrees(t)
	firstCommand, firstOutput := lintCommand(t, first, cacheRoot, filepath.Join(filepath.Dir(seed), "run-first"))
	secondCommand, secondOutput := lintCommand(t, second, cacheRoot, filepath.Join(filepath.Dir(seed), "run-second"))
	require.NoError(t, firstCommand.Start())
	require.NoError(t, secondCommand.Start())
	require.NoError(t, firstCommand.Wait(), firstOutput.String())
	require.NoError(t, secondCommand.Wait(), secondOutput.String())

	firstRoot := resolvedPath(t, first)
	secondRoot := resolvedPath(t, second)
	require.NotContains(t, firstOutput.String(), secondRoot)
	require.NotContains(t, secondOutput.String(), firstRoot)
	require.Contains(t, firstOutput.String(), "x.go:4:2: ineffectual assignment")
	require.Contains(t, secondOutput.String(), "x.go:4:2: ineffectual assignment")
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

	command := testenv.Command(t,
		"bash", validationEnvPath(t), "--ephemeral", "sh", "-c", "printf %s \"$VALIDATION_RUN_DIR\"",
	)
	command.Dir = repository
	command.Env = testenv.Environment(
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
	command := testenv.Command(t,
		"bash", validationEnvPath(t), runDirectory, "sh", "-c",
		"env | grep -E '^(HOME|GOCACHE|GOMODCACHE|GOPATH|TMPDIR|XDG_CACHE_HOME|GOLANGCI_LINT_CACHE|LEDGER_AI_CACHE_ROOT)='",
	)
	command.Dir = repository
	command.Env = testenv.Environment("HOME="+userHome, "LEDGER_AI_CACHE_ROOT="+cacheRoot)
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
	command.Env = testenv.Environment(
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

func newLintWorktrees(t *testing.T) (string, string, string, string) {
	t.Helper()

	root := t.TempDir()
	seed := filepath.Join(root, "seed")
	producer := filepath.Join(root, "producer")
	consumer := filepath.Join(root, "consumer")
	cacheRoot := filepath.Join(root, "cache")
	require.NoError(t, os.MkdirAll(seed, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(seed, "go.mod"), []byte("module example.test/cacheleak\n\ngo 1.24\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(seed, "x.go"), []byte("package cacheleak\n\nfunc Value() int {\n\tvalue := 1\n\tvalue = 2\n\treturn value\n}\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(seed, ".golangci.yml"), []byte("version: \"2\"\nlinters:\n  default: none\n  enable: [ineffassign]\n"), 0o644))
	runGit(t, seed, "init", "-q")
	runGit(t, seed, "config", "user.email", "repro@example.test")
	runGit(t, seed, "config", "user.name", "repro")
	runGit(t, seed, "add", "go.mod", "x.go", ".golangci.yml")
	runGit(t, seed, "commit", "-qm", "initial")
	runGit(t, seed, "worktree", "add", "--detach", "-q", producer, "HEAD")
	runGit(t, seed, "worktree", "add", "--detach", "-q", consumer, "HEAD")

	return seed, producer, consumer, cacheRoot
}

func runLint(t *testing.T, repository, cacheRoot, runDirectory string, extraArguments ...string) string {
	t.Helper()

	command, output := lintCommand(t, repository, cacheRoot, runDirectory, extraArguments...)
	require.NoError(t, command.Run(), output.String())

	return output.String()
}

func lintCommand(t *testing.T, repository, cacheRoot, runDirectory string, extraArguments ...string) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	arguments := []string{validationEnvPath(t), runDirectory, "golangci-lint", "run", "-v", "--issues-exit-code", "0"}
	arguments = append(arguments, extraArguments...)
	arguments = append(arguments, "./...")
	command := exec.CommandContext(ctx, "bash", arguments...)
	command.Dir = repository
	command.Env = testenv.Environment(
		"LEDGER_AI_CACHE_ROOT="+cacheRoot,
		"GL_DEBUG=goanalysis/issues/cache",
	)
	output := &bytes.Buffer{}
	command.Stdout = output
	command.Stderr = output

	return command, output
}

func resolvedPath(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	require.NoError(t, err)

	return resolved
}

func diagnosticLines(output string) []string {
	var lines []string
	for line := range strings.SplitSeq(output, "\n") {
		if strings.Contains(line, ": ineffectual assignment") {
			lines = append(lines, line)
		}
	}

	return lines
}

func runGit(t *testing.T, directory string, arguments ...string) {
	t.Helper()
	command := testenv.Command(t, "git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, fmt.Sprintf("git %s:\n%s", strings.Join(arguments, " "), output))
}
