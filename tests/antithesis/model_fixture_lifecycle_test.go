package antithesis_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestModelFixtureEnvironment(t *testing.T) {
	t.Parallel()

	got := modelFixtureEnvironment([]string{
		"PATH=/trusted/bin", "HOME=/fixture/home", "TMPDIR=/fixture/tmp", "SYSTEMROOT=C:\\Windows",
		"NODES=3", "RESTORE=1", "BASH_ENV=/untrusted/startup", "ENV=/untrusted/startup",
		"EXPECTED_HEAD=outer-head", "AI_REVIEW_BASE_SHA=outer-base", "VALIDATION_RUN_ID=outer-run",
		"MODEL_FAIL_FAST=0", "MODEL_TEST_HELPER=1", "FAKE_MODEL_SCENARIO=wrong", "AUTH_ENABLED=true",
	})
	require.Equal(t, []string{
		"PATH=/trusted/bin", "HOME=/fixture/home", "TMPDIR=/fixture/tmp", "SYSTEMROOT=C:\\Windows",
	}, got)
}

func TestModelFixtureCancellationKillsProcessGroup(t *testing.T) {
	t.Parallel()

	executable, err := os.Executable()
	require.NoError(t, err)
	readyReader, readyWriter, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = readyReader.Close() // Best-effort cleanup after the readiness reader finishes.
		_ = readyWriter.Close() // The parent's copy is normally closed immediately after Start.
	})

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable)
	cmd.Args[0] = "cancellation-parent"
	cmd.Env = append(modelFixtureEnvironment(os.Environ()), "MODEL_TEST_HELPER=1")
	cmd.ExtraFiles = []*os.File{readyWriter}
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	configureModelFixtureProcess(cmd)
	require.NoError(t, cmd.Start())
	waited := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		waited <- cmd.Wait()
		close(done)
	}()
	// Keep a cleanup independent of the mechanism under test: a mutation that
	// kills only the parent must not leave the child running after this test.
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) // Best-effort cleanup of any surviving fixture descendants.
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("fixture process did not finish during cleanup")
		}
	})
	require.NoError(t, readyWriter.Close())

	type readiness struct {
		pid int
		err error
	}
	ready := make(chan readiness, 1)
	go func() {
		var childPID int
		_, err := fmt.Fscanln(readyReader, &childPID)
		ready <- readiness{pid: childPID, err: err}
	}()

	var childPID int
	select {
	case result := <-ready:
		require.NoError(t, result.err)
		childPID = result.pid
	case err := <-waited:
		t.Fatalf("fixture parent exited before child readiness: %v; %s", err, output.String())
	case <-ctx.Done():
		t.Fatal("fixture child did not become ready before the deadline")
	}
	require.Positive(t, childPID)
	for _, pid := range []int{cmd.Process.Pid, childPID} {
		groupID, err := syscall.Getpgid(pid)
		require.NoError(t, err)
		require.Equal(t, cmd.Process.Pid, groupID, "process %d must belong to the fixture group", pid)
	}

	cancel()
	select {
	case err := <-waited:
		require.Error(t, err, "cancellation must terminate the running fixture")
	case <-time.After(3 * time.Second):
		t.Fatal("waiting for the cancelled fixture did not finish")
	}
	for _, pid := range []int{cmd.Process.Pid, childPID} {
		require.Eventually(t, func() bool {
			return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
		}, 3*time.Second, 10*time.Millisecond, "process %d survived fixture cancellation", pid)
	}
}

// The helper confirms that its real child has started before reporting the PID
// to the test. It then remains alive waiting for that child, so cancellation
// must stop both processes rather than merely an already-exited parent.
func runModelFixtureCancellationParent() error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	child := exec.Command(executable)
	child.Args[0] = "ledger-server"
	child.Env = os.Environ()
	child.Stderr = os.Stderr
	stdout, err := child.StdoutPipe()
	if err != nil {
		return err
	}
	if err := child.Start(); err != nil {
		return err
	}
	defer func() {
		if child.ProcessState == nil {
			_ = child.Process.Kill() // Best-effort cleanup if readiness fails.
			_ = child.Wait()         // Reap the killed child; its failure is expected during cleanup.
		}
	}()
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		return fmt.Errorf("reading child readiness: %w", err)
	}
	if strings.TrimSpace(line) != "Became leader" {
		return fmt.Errorf("unexpected child readiness: %q", line)
	}
	ready := os.NewFile(3, "child-ready")
	if _, err := fmt.Fprintln(ready, child.Process.Pid); err != nil {
		return err
	}
	if err := ready.Close(); err != nil {
		return err
	}
	return child.Wait()
}
