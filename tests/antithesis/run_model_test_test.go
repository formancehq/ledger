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

	fakeGo := filepath.Join(binDir, "go")
	fakeSeq := filepath.Join(binDir, "seq")
	fakeServer := filepath.Join(tempDir, "fake-server")
	fakeDriver := filepath.Join(tempDir, "fake-driver")
	writeExecutable(t, fakeGo, `#!/bin/sh
out=
previous=
for arg in "$@"; do
	if [ "$previous" = "-o" ]; then out="$arg"; break; fi
	previous="$arg"
done
case "$out" in
	*/ledger-server) cp "$FAKE_SERVER_BIN" "$out" ;;
	*/model-driver) cp "$FAKE_DRIVER_BIN" "$out" ;;
	*) echo "unexpected fake go invocation: $*" >&2; exit 1 ;;
esac
chmod +x "$out"
`)
	writeExecutable(t, fakeSeq, `#!/bin/sh
if [ "$#" -eq 2 ] && [ "$1" = "1" ] && [ "$2" = "0" ]; then exit 0; fi
exec /usr/bin/seq "$@"
`)
	writeExecutable(t, fakeServer, `#!/bin/sh
echo "Became leader"
trap 'exit 0' TERM INT
while :; do sleep 1; done
`)
	writeExecutable(t, fakeDriver, `#!/bin/sh
write_assertion() {
	printf '%s\n' "$1" >>"$ANTITHESIS_SDK_LOCAL_OUTPUT"
}
stay_alive() {
	trap 'exit 0' TERM INT
	while :; do sleep 1; done
}
case "$FAKE_MODEL_SCENARIO" in
	blocked)
		write_assertion '{"antithesis_assert":{"display_type":"Reachable","message":"singleton_driver_model: model outcome verified","condition":true,"hit":false}}'
		echo "first setup Apply entered"
		stay_alive
		echo "first setup Apply completed"
		;;
	early-exit)
		exit 0
		;;
	registration-only)
		write_assertion '{"antithesis_assert":{"display_type":"Reachable","message":"singleton_driver_model: model outcome verified","condition":true,"hit":false}}'
		stay_alive
		;;
	setup-only)
		write_assertion '{"antithesis_assert":{"display_type":"Sometimes","message":"should be able to create ledger","condition":true,"hit":true}}'
		write_assertion '{"antithesis_assert":{"display_type":"Sometimes","message":"should always be able to get created ledger","condition":true,"hit":true}}'
		stay_alive
		;;
	unverified-output)
		write_assertion '{"antithesis_assert":{"display_type":"Reachable","message":"singleton_driver_model: unrelated path exercised","condition":true,"hit":true}}'
		write_assertion '{"antithesis_assert":{"display_type":"Reachable","message":"singleton_driver_model: model outcome verified","condition":true,"hit":false}}'
		stay_alive
		;;
	verified)
		write_assertion '{"antithesis_assert":{"display_type":"Reachable","message":"singleton_driver_model: model outcome verified","condition":true,"hit":true}}'
		stay_alive
		;;
	*)
		echo "unknown scenario: $FAKE_MODEL_SCENARIO" >&2
		exit 2
		;;
esac
`)

	packageDir, err := os.Getwd()
	require.NoError(t, err)
	runner := filepath.Join(packageDir, "run_model_test.sh")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, runner, "2")
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"IN_NIX_SHELL=1",
		"KEEP_WORKDIR=1",
		"TMPDIR="+tempDir,
		"REPO="+repoDir,
		"MODEL_HARNESS_REPO="+harnessDir,
		"FAKE_SERVER_BIN="+fakeServer,
		"FAKE_DRIVER_BIN="+fakeDriver,
		"FAKE_MODEL_SCENARIO="+scenario,
	)
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

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o755))
}
