//go:build !windows

package cmdutil_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"testing"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/accounts"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/transactions"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// Exercise the real pager, after it has canceled its first RPC context. Each
// subprocess owns a private PTY; readiness is the displayed prompt, not a sleep.
func TestListPagerRestoresTerminal(t *testing.T) {
	const helper = "LEDGER_PAGINATION_PROMPT_HELPER"
	if selected := os.Getenv(helper); selected != "" {
		resource, action, ok := strings.Cut(selected, "/")
		require.True(t, ok)
		runListPagerPrompt(t, resource, action)
		return
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
    child = subprocess.Popen([sys.argv[1], "-test.run=^TestListPagerRestoresTerminal$"],
                             stdin=slave, stdout=subprocess.PIPE, stderr=slave)
    observed = b""
    deadline = time.monotonic() + 5
    while b"(Y/n)" not in observed:
        remaining = deadline - time.monotonic()
        assert remaining > 0, ("pager prompt did not start", observed)
        ready, _, _ = select.select([master], [], [], remaining)
        assert ready, ("pager prompt did not start", observed)
        observed += os.read(master, 4096)
    assert b"Load next page?" in observed, observed
    assert b"q to quit" in observed, observed
    raw = termios.tcgetattr(slave)
    assert not raw[3] & termios.ICANON, "pager did not enter raw input mode"
    if sys.argv[2] == "sigterm":
        child.send_signal(signal.SIGTERM)
    else:
        os.write(master, {"next": b"\r", "yes": b"y", "quit": b"q", "no": b"n"}[sys.argv[2]])
    stdout, _ = child.communicate(timeout=5)
    assert child.returncode == 0, (child.returncode, stdout, observed)
    after = termios.tcgetattr(slave)
    # Restoring canonical input can set the kernel's transient PENDIN marker.
    before[3] &= ~getattr(termios, "PENDIN", 0)
    after[3] &= ~getattr(termios, "PENDIN", 0)
    assert after == before, ("pager left terminal attributes changed", before, after)
    assert b"Previous page" in stdout, stdout
    assert b"More results available" in stdout, stdout
finally:
    if child is not None and child.poll() is None:
        child.kill()
        child.wait()
    os.close(master)
    os.close(slave)
`
	for _, resource := range []string{"accounts", "transactions"} {
		for _, action := range []string{"sigterm", "next", "yes", "quit", "no"} {
			t.Run(resource+"/"+action, func(t *testing.T) {
				t.Parallel()
				command := exec.CommandContext(t.Context(), python, "-c", script, os.Args[0], action)
				command.Env = append(os.Environ(), helper+"="+resource+"/"+action)
				output, err := command.CombinedOutput()
				require.NoError(t, err, string(output))
			})
		}
	}
}

func runListPagerPrompt(t *testing.T, resource, action string) {
	t.Helper()
	ctx, stop := signal.NotifyContext(t.Context(), syscall.SIGTERM)
	defer stop()
	cmd := accounts.NewListCommand()
	if resource == "transactions" {
		cmd = transactions.NewListCommand()
	}
	cmd.SetContext(ctx)
	cmd.SetIn(os.Stdin)
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(os.Stderr)
	calls := 0
	pageResult := func(page cmdutil.PaginationFlags) metadata.MD {
		calls++
		if calls == 1 {
			require.Empty(t, page.Cursor)
			return metadata.Pairs(cmdutil.NextCursorTrailerKey, "next", cmdutil.PreviousCursorTrailerKey, "previous")
		}
		require.Equal(t, 2, calls, "pager fetched an unexpected additional page")
		require.Equal(t, "next", page.Cursor)
		return nil
	}
	var err error
	if resource == "accounts" {
		err = accounts.RunListWithFetch(cmd, func(_ context.Context, page cmdutil.PaginationFlags) ([]*commonpb.Account, metadata.MD, error) {
			trailer := pageResult(page)
			return []*commonpb.Account{{Address: fmt.Sprintf("pager-row-%d", calls)}}, trailer, nil
		})
	} else {
		err = transactions.RunListWithFetch(cmd, "books", func(_ context.Context, page cmdutil.PaginationFlags) ([]*commonpb.Transaction, metadata.MD, error) {
			trailer := pageResult(page)
			return []*commonpb.Transaction{{Id: uint64(calls), Reference: fmt.Sprintf("pager-row-%d", calls)}}, trailer, nil
		})
	}
	if action == "sigterm" {
		require.ErrorIs(t, err, context.Canceled)
	} else {
		require.NoError(t, err)
	}
	wantCalls := 1
	if action == "next" || action == "yes" {
		wantCalls = 2
		require.Contains(t, output.String(), "pager-row-2")
	}
	require.Equal(t, wantCalls, calls)
	require.Contains(t, output.String(), "pager-row-1", "the first native page must remain on the scoped output")
	require.NotContains(t, output.String(), "Load next page?", "the prompt belongs on the diagnostic writer")
}
