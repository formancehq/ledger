//go:build !windows

package cmdutil

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestPromptInputCanceledContextLeavesInputUnread(t *testing.T) {
	t.Parallel()
	input, output, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = input.Close(); _ = output.Close() })
	_, err = output.WriteString("a")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	reader := &promptInput{ctx: ctx, file: input}
	var buffer [1]byte
	n, err := reader.Read(buffer[:])
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, n)
	_, err = input.Read(buffer[:])
	require.NoError(t, err)
	require.Equal(t, byte('a'), buffer[0])
}

func TestAskOneContextUnwindsPromptBeforeReturningCancellation(t *testing.T) {
	t.Parallel()
	input, output, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = input.Close(); _ = output.Close() })
	flags, err := unix.FcntlInt(input.Fd(), unix.F_GETFL, 0)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	prompt := &readCreatePrompt{entered: make(chan struct{}), restored: make(chan struct{})}
	answer := "unchanged"
	done := make(chan error, 1)
	go func() {
		done <- AskOneContext(ctx, prompt, &answer, input, output, io.Discard)
	}()
	select {
	case <-prompt.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("prompt did not start")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("prompt did not return on cancellation")
	}
	select {
	case <-prompt.restored:
	default:
		t.Fatal("returned before prompt restoration completed")
	}
	require.Equal(t, "unchanged", answer)
	actualFlags, err := unix.FcntlInt(input.Fd(), unix.F_GETFL, 0)
	require.NoError(t, err)
	require.Equal(t, flags, actualFlags)
}

func TestAskOneContextPreservesNormalAnswer(t *testing.T) {
	t.Parallel()
	input, output, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = input.Close(); _ = output.Close() })
	_, err = output.WriteString("a")
	require.NoError(t, err)
	prompt := &readCreatePrompt{entered: make(chan struct{}), restored: make(chan struct{})}
	var answer string
	require.NoError(t, AskOneContext(t.Context(), prompt, &answer, input, output, io.Discard))
	require.Equal(t, "a", answer)
}

type readCreatePrompt struct {
	survey.Input

	entered  chan struct{}
	restored chan struct{}
}

func (prompt *readCreatePrompt) Prompt(*survey.PromptConfig) (any, error) {
	defer close(prompt.restored)
	close(prompt.entered)
	var buffer [1]byte
	_, err := prompt.Stdio().In.Read(buffer[:])

	return string(buffer[:]), err
}

func (*readCreatePrompt) Cleanup(*survey.PromptConfig, any) error { return nil }

func TestAskOneContextRestoresTerminalOnSIGTERM(t *testing.T) {
	const helper = "LEDGER_CREATE_PROMPT_SIGTERM_HELPER"
	if os.Getenv(helper) == "1" {
		ctx, stop := signal.NotifyContext(t.Context(), syscall.SIGTERM)
		var answer string
		err := AskOneContext(ctx, &survey.Input{Message: "Ledger name"}, &answer, os.Stdin, os.Stderr, os.Stderr)
		stop()
		require.ErrorIs(t, err, context.Canceled)
		os.Exit(0)
	}
	t.Parallel()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is required for the private PTY regression")
	}
	const script = `
import os, pty, select, signal, subprocess, sys, termios, time
master, slave = pty.openpty()
child = None
try:
    before = termios.tcgetattr(slave)
    child = subprocess.Popen([sys.argv[1], "-test.run=^TestAskOneContextRestoresTerminalOnSIGTERM$"],
                             stdin=slave, stdout=subprocess.PIPE, stderr=slave)
    observed = b""
    deadline = time.monotonic() + 5
    while b"\x1b[6n" not in observed:
        remaining = deadline - time.monotonic()
        assert remaining > 0, "Survey did not request cursor position"
        ready, _, _ = select.select([master], [], [], remaining)
        assert ready, "Survey did not start"
        observed += os.read(master, 4096)
    raw = termios.tcgetattr(slave)
    assert not raw[3] & termios.ICANON, "Survey did not enter raw input mode"
    child.send_signal(signal.SIGTERM)
    stdout, _ = child.communicate(timeout=5)
    assert child.returncode == 0, (child.returncode, stdout, observed)
    after = termios.tcgetattr(slave)
    # Restoring canonical input can set the kernel's transient PENDIN marker.
    before[3] &= ~getattr(termios, "PENDIN", 0)
    after[3] &= ~getattr(termios, "PENDIN", 0)
    assert after == before, ("Survey left terminal attributes changed", before, after)
finally:
    if child is not None and child.poll() is None:
        child.kill()
        child.wait()
    os.close(master)
    os.close(slave)
`
	command := exec.CommandContext(t.Context(), python, "-c", script, os.Args[0])
	command.Env = append(os.Environ(), helper+"=1")
	var diagnostics bytes.Buffer
	command.Stderr = &diagnostics
	require.NoError(t, command.Run(), diagnostics.String())
}
