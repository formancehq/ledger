package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/scripts/internal/testenv"
)

func TestMain(testingMain *testing.M) {
	if err := testenv.SanitizeProcess(); err != nil {
		panic(err)
	}
	if err := os.Setenv("GIT_CONFIG_GLOBAL", os.DevNull); err != nil {
		panic(err)
	}
	if err := os.Setenv("GIT_CONFIG_SYSTEM", os.DevNull); err != nil {
		panic(err)
	}
	os.Exit(testingMain.Run())
}

func TestRunnerAlwaysTakesTwoSnapshots(t *testing.T) {
	t.Parallel()

	root := newRunnerTestRepository(t)
	for _, test := range []struct {
		name       string
		child      string
		wantStatus int
	}{
		{name: "success", child: "exit 0", wantStatus: 0},
		{name: "child failure", child: "exit 7", wantStatus: 7},
		{name: "child cancellation", child: "kill -TERM $$", wantStatus: exitError},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			status := run([]string{"--root", root, "--", "/bin/sh", "-c", test.child}, strings.NewReader(""), &output, &output)
			require.Equal(t, test.wantStatus, status, output.String())
			require.Equal(t, 1, strings.Count(output.String(), "ROOT_PROTECTION_ARMED"))
			require.Equal(t, 1, strings.Count(output.String(), "ROOT_SNAPSHOT_CAPTURED position=after"))
			require.Equal(t, 1, strings.Count(output.String(), "ROOT_UNCHANGED=PASS"))
			require.Contains(t, output.String(), "gitProcesses=5")
			require.Contains(t, output.String(), "ignoredEntries=0")
		})
	}
}

func TestRunnerDetectsPersistentPrimaryMutationOnEveryChildExit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
	}{
		{name: "tracked", script: "printf changed > base.txt"},
		{name: "staged", script: "printf changed > base.txt; git add base.txt"},
		{name: "untracked", script: "printf changed > untracked.txt"},
		{name: "branch", script: "git switch -c moved"},
		{name: "child failure", script: "printf changed > base.txt; exit 7"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := newRunnerTestRepository(t)
			var output bytes.Buffer
			child := "cd " + shellQuote(root) + "; " + test.script
			status := run([]string{"--root", root, "--", "/bin/sh", "-c", child}, strings.NewReader(""), &output, &output)
			require.Equal(t, exitError, status, output.String())
			require.Contains(t, output.String(), "ROOT_MUTATION_DETECTED")
			require.NotContains(t, output.String(), "ROOT_UNCHANGED=PASS")
		})
	}
}

func TestRunnerAllowsIgnoredSharedCacheChurn(t *testing.T) {
	t.Parallel()

	root := newRunnerTestRepository(t)
	var output bytes.Buffer
	child := "cd " + shellQuote(root) + "; printf changed > ignored/value"
	status := run([]string{"--root", root, "--", "/bin/sh", "-c", child}, strings.NewReader(""), &output, &output)
	require.Equal(t, 0, status, output.String())
	require.Contains(t, output.String(), "ROOT_UNCHANGED=PASS")
}

func TestRunnerComparesRootAfterCancellation(t *testing.T) {
	root := newRunnerTestRepository(t)
	binary := filepath.Join(t.TempDir(), "rootguard")
	build := testenv.Command(t, "go", "build", "-o", binary, ".")
	build.Dir = packageRoot(t)
	output, err := build.CombinedOutput()
	require.NoError(t, err, string(output))

	ready := filepath.Join(t.TempDir(), "ready")
	command := exec.Command(binary, "--root", root, "--", "/bin/sh", "-c", "printf ready > "+shellQuote(ready)+"; while :; do sleep 1; done")
	command.Dir = root
	command.Env = testenv.Environment()
	var combined bytes.Buffer
	command.Stdout = &combined
	command.Stderr = &combined
	require.NoError(t, command.Start())
	require.Eventually(t, func() bool {
		_, err := os.Stat(ready)

		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, command.Process.Signal(syscall.SIGTERM))
	done := make(chan error, 1)
	go func() {
		done <- command.Wait()
	}()
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		_ = command.Process.Kill() // Best effort cleanup before failing the bounded regression.
		t.Fatal("rootguard did not finish after cancellation")
	}
	require.Error(t, err, combined.String())
	require.Equal(t, exitError, command.ProcessState.ExitCode(), combined.String())
	require.Contains(t, combined.String(), "ROOT_UNCHANGED=PASS")
	require.Contains(t, combined.String(), "ROOT_SNAPSHOT_CAPTURED position=after")
}

func newRunnerTestRepository(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "root")
	require.NoError(t, os.Mkdir(root, 0o755))
	runRunnerGit(t, root, "init")
	runRunnerGit(t, root, "config", "user.name", "Root Guard Runner Test")
	runRunnerGit(t, root, "config", "user.email", "rootguard-runner@example.com")
	require.NoError(t, os.WriteFile(filepath.Join(root, ".gitignore"), []byte("ignored/\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "base.txt"), []byte("base\n"), 0o644))
	require.NoError(t, os.Mkdir(filepath.Join(root, "ignored"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "ignored", "value"), []byte("before"), 0o644))
	runRunnerGit(t, root, "add", ".gitignore", "base.txt")
	runRunnerGit(t, root, "commit", "-m", "base")

	return root
}

func runRunnerGit(t *testing.T, directory string, arguments ...string) {
	t.Helper()
	command := testenv.Command(t, "git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, "git %s:\n%s", strings.Join(arguments, " "), output)
}

func packageRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	require.NoError(t, err)

	return directory
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
