package antithesis_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunModelTestRequiresVerifiedOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scenario   string
		wantPass   bool
		wantOutput string
	}{
		{name: "driver blocked in first setup Apply", scenario: "blocked", wantOutput: "NO VERIFIED MODEL OUTCOMES"},
		{name: "driver exits before deadline", scenario: "early-exit", wantOutput: "DRIVER EXITED EARLY"},
		{name: "registration only", scenario: "registration-only", wantOutput: "NO VERIFIED MODEL OUTCOMES"},
		{name: "setup assertions only", scenario: "setup-only", wantOutput: "NO VERIFIED MODEL OUTCOMES"},
		{name: "output without verified hit", scenario: "unverified-output", wantOutput: "NO VERIFIED MODEL OUTCOMES"},
		{name: "completed model result", scenario: "verified", wantPass: true, wantOutput: "RESULT: PASS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			output, driverLog, err := runModelTestFixture(t, tt.scenario)
			if tt.wantPass {
				require.NoError(t, err, output)
			} else {
				require.Error(t, err, output)
			}
			require.Contains(t, output, tt.wantOutput)

			if tt.scenario == "blocked" {
				require.Contains(t, driverLog, "first setup Apply entered")
				require.NotContains(t, driverLog, "first setup Apply completed")
				require.NotContains(t, output, "driver exited early")
			}
		})
	}
}

func runModelTestFixture(t *testing.T, scenario string) (string, string, error) {
	t.Helper()

	tempDir := t.TempDir()
	repoDir := filepath.Join(tempDir, "repo")
	harnessDir := filepath.Join(tempDir, "harness")
	binDir := filepath.Join(tempDir, "bin")
	require.NoError(t, os.MkdirAll(repoDir, 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(harnessDir, "tests", "antithesis", "workload"), 0o755))
	require.NoError(t, os.MkdirAll(binDir, 0o755))

	executable, err := os.Executable()
	require.NoError(t, err)
	for _, name := range []string{"go", "seq", "date"} {
		require.NoError(t, os.Symlink(executable, filepath.Join(binDir, name)))
	}
	readyReader, readyWriter, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, readyReader.Close())
		require.NoError(t, readyWriter.Close())
	})
	date, err := exec.LookPath("date")
	require.NoError(t, err)
	seq, err := exec.LookPath("seq")
	require.NoError(t, err)

	packageDir, err := os.Getwd()
	require.NoError(t, err)
	runner := filepath.Join(packageDir, "run_model_test.sh")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", runner, "2")
	cmd.Env = append(modelFixtureEnvironment(os.Environ()),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"IN_NIX_SHELL=1",
		"KEEP_WORKDIR=1",
		"TMPDIR="+tempDir,
		"REPO="+repoDir,
		"MODEL_HARNESS_REPO="+harnessDir,
		"MODEL_TEST_HELPER=1",
		"MODEL_TEST_EXECUTABLE="+executable,
		"MODEL_TEST_DATE="+date,
		"MODEL_TEST_SEQ="+seq,
		"GORACE=atexit_sleep_ms=0",
		"FAKE_MODEL_SCENARIO="+scenario,
	)
	// The clock waits for scenario evidence before the two-second run starts.
	cmd.ExtraFiles = []*os.File{readyReader, readyWriter}
	configureModelFixtureProcess(cmd)
	combined, err := cmd.CombinedOutput()
	require.NoError(t, ctx.Err(), string(combined))

	workDirs, globErr := filepath.Glob(filepath.Join(tempDir, "model-test.*"))
	require.NoError(t, globErr)
	require.Len(t, workDirs, 1, string(combined))
	driverLogPath := filepath.Join(workDirs[0], "driver.log")
	require.FileExists(t, driverLogPath, string(combined))
	driverLog, readErr := os.ReadFile(driverLogPath)
	require.NoError(t, readErr, string(combined))

	return string(combined), strings.TrimSpace(string(driverLog)), err
}
