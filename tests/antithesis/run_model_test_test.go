package antithesis_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
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

			output, driverLog, err, fixtureErr := runModelTestFixture(t, tt.scenario)
			require.NoError(t, fixtureErr, output)
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

func runModelTestFixture(t *testing.T, scenario string) (string, string, error, error) {
	t.Helper()

	tempDir := t.TempDir()
	repoDir := filepath.Join(tempDir, "repo")
	harnessDir := filepath.Join(tempDir, "harness")
	binDir := filepath.Join(tempDir, "bin")
	require.NoError(t, os.MkdirAll(repoDir, 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(harnessDir, "tests", "antithesis", "workload"), 0o755))
	require.NoError(t, os.MkdirAll(binDir, 0o755))

	// Only the fixture clock is substituted: liveness checks and reporting run
	// unchanged. File descriptors 3/4 carry events/clock replies, respectively.
	writeExecutable(t, filepath.Join(binDir, "date"), `#!/bin/sh
if [ "$#" -ne 1 ] || [ "$1" != '+%s' ]; then
	printf 'invalid-date\n' >&3
	exit 1
fi
printf 'clock\n' >&3
read -r now <&4 || exit 1
printf '%s\n' "$now"
`)
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
trap 'printf "driver-exit %s\n" "$?" >&3' EXIT
write_assertion() {
	printf '%s\n' "$1" >>"$ANTITHESIS_SDK_LOCAL_OUTPUT"
}
stay_alive() {
	trap 'exit 0' TERM INT
	printf 'ready\n' >&3
	while :; do sleep 1; done
}
case "$FAKE_MODEL_SCENARIO" in
	blocked)
		write_assertion '{"antithesis_assert":{"display_type":"Reachable","message":"singleton_driver_model: model outcome verified","condition":true,"hit":false}}'
		echo "first setup Apply entered"
		stay_alive
		echo "first setup Apply completed"
		;;
	fail-before-ready)
		echo "injected driver failure before readiness"
		exit 7
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
		"NODES=1",
		"RESTORE=0",
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
	combined, err, fixtureErr := runModelFixtureCommand(ctx, cmd, scenario)

	workDirs, globErr := filepath.Glob(filepath.Join(tempDir, "model-test.*"))
	require.NoError(t, globErr)
	if len(workDirs) != 1 {
		fixtureErr = errors.Join(fixtureErr, fmt.Errorf("expected one runner workdir, got %d", len(workDirs)))
	}
	var driverLog string
	for _, workDir := range workDirs {
		var combinedSb187 strings.Builder
		for _, name := range []string{"driver.log", "server-0.log"} {
			data, readErr := os.ReadFile(filepath.Join(workDir, name))
			if readErr != nil {
				combinedSb187.WriteString(fmt.Sprintf("\n%s: %v\n", name, readErr))
				fixtureErr = errors.Join(fixtureErr, readErr)

				continue
			}
			combinedSb187.WriteString(fmt.Sprintf("\n%s:\n%s", name, data))
			if name == "driver.log" {
				driverLog = strings.TrimSpace(string(data))
			}
		}
		combined += combinedSb187.String()
	}

	return combined, driverLog, err, fixtureErr
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o755))
}

// modelFixtureClock holds time before the deadline until the expected driver
// state exists. The first two queries initialize deadline/restart bookkeeping;
// subsequent queries guard the monitor. One live iteration follows readiness.
// Early-exit never advances time: only the runner's real liveness check may
// finish that scenario. The context deadline remains an infrastructure failure.
type modelFixtureClock struct {
	scenario      string
	queries       int
	ready         bool
	observedReady bool
	expired       bool
	exiting       bool
}

func (clock *modelFixtureClock) event(event string) (string, error) {
	switch event {
	case "clock":
		clock.queries++
		if clock.scenario == "early-exit" || !clock.ready || clock.queries <= 2 {
			return "100\n", nil
		}
		if !clock.observedReady {
			clock.observedReady = true

			return "100\n", nil
		}
		clock.expired = true

		return "102\n", nil
	case "ready":
		if clock.ready || clock.exiting || clock.scenario == "early-exit" {
			return "", fmt.Errorf("unexpected driver readiness: %+v", clock)
		}
		clock.ready = true

		return "", nil
	case "driver-exit 0":
		if clock.exiting || (clock.scenario != "early-exit" && !clock.expired) {
			return "", errors.New("driver exited before the fixture deadline")
		}
		clock.exiting = true

		return "", nil
	default:
		return "", fmt.Errorf("unexpected fixture event %q", event)
	}
}

func runModelFixtureCommand(ctx context.Context, cmd *exec.Cmd, scenario string) (string, error, error) {
	eventsReader, eventsWriter, err := os.Pipe()
	if err != nil {
		return "", nil, err
	}
	defer func() { _ = eventsReader.Close() }() // Also interrupts the scanner on abort.
	defer func() { _ = eventsWriter.Close() }() // Covers failure before Start.
	clockReader, clockWriter, err := os.Pipe()
	if err != nil {
		return "", nil, err
	}
	defer func() { _ = clockReader.Close() }() // Covers failure before Start.
	defer func() { _ = clockWriter.Close() }()
	cmd.ExtraFiles = []*os.File{eventsWriter, clockReader}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}

		return err
	}
	cmd.WaitDelay = time.Second
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		return "", nil, err
	}
	_ = eventsWriter.Close() // Only descendants own the event writer now.
	_ = clockReader.Close()  // Only fake date consumes replies.
	finished := make(chan error, 1)
	go func() { finished <- cmd.Wait() }()
	events := make(chan string)
	stop := make(chan struct{})
	defer close(stop)
	go func(events chan<- string) {
		defer close(events)
		scanner := bufio.NewScanner(eventsReader)
		for scanner.Scan() {
			select {
			case events <- scanner.Text():
			case <-stop:
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case events <- "event pipe: " + err.Error():
			case <-stop:
			}
		}
	}(events)
	clock := modelFixtureClock{scenario: scenario}
	var fixtureErr, runnerErr error
	runnerDone := false
	for fixtureErr == nil && (!runnerDone || events != nil) {
		select {
		case event, ok := <-events:
			if !ok {
				events = nil // EOF is not an exit notification; wait for cmd.Wait.

				continue
			}
			var reply string
			reply, fixtureErr = clock.event(event)
			if fixtureErr == nil && reply != "" {
				_, fixtureErr = io.WriteString(clockWriter, reply)
			}
		case runnerErr = <-finished:
			runnerDone = true
			finished = nil
			// Wait and pipe delivery are independent. Drain events before deciding
			// success so an exit/protocol error cannot be lost to select ordering.
		case <-ctx.Done():
			fixtureErr = ctx.Err()
		}
	}
	if fixtureErr != nil {
		if err := cmd.Cancel(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			fixtureErr = errors.Join(fixtureErr, err)
		}
	} else if scenario != "early-exit" && !clock.expired {
		fixtureErr = errors.New("runner exited before driver readiness and the fixture deadline")
	}
	if !runnerDone {
		runnerErr = <-finished // Reap and finish output collection before reading logs.
	}

	return output.String(), runnerErr, fixtureErr
}

func TestRunModelFixtureReportsDriverFailure(t *testing.T) {
	t.Parallel()
	output, driverLog, _, err := runModelTestFixture(t, "fail-before-ready")
	require.ErrorContains(t, err, `unexpected fixture event "driver-exit 7"`, output)
	require.NotErrorIs(t, err, context.DeadlineExceeded, output)
	require.Contains(t, driverLog, "injected driver failure before readiness")
	require.Contains(t, output, "server-0.log:")
	require.Contains(t, output, "Became leader")
}

func TestModelFixtureClockWaitsForReadiness(t *testing.T) {
	t.Parallel()
	clock := modelFixtureClock{scenario: "verified"}
	// Repeated monitor probes before driver startup cannot consume its budget.
	for range 5 {
		reply, err := clock.event("clock")
		require.NoError(t, err)
		require.Equal(t, "100\n", reply)
	}
	_, err := clock.event("ready")
	require.NoError(t, err)
	reply, err := clock.event("clock")
	require.NoError(t, err)
	require.Equal(t, "100\n", reply)
	reply, err = clock.event("clock")
	require.NoError(t, err)
	require.Equal(t, "102\n", reply)
	_, err = clock.event("ready")
	require.ErrorContains(t, err, "unexpected driver readiness")
	_, err = clock.event("invalid-date")
	require.ErrorContains(t, err, "unexpected fixture event")
}

func TestModelFixtureDrainsEventsAfterRunnerExit(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", `printf 'driver-exit 7\n' >&3; exit 7`)
	_, _, err := runModelFixtureCommand(ctx, cmd, "verified")
	require.ErrorContains(t, err, `unexpected fixture event "driver-exit 7"`)
}
