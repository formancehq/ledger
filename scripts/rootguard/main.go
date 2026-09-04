package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/formancehq/ledger/v3/scripts/internal/rootguard"
)

const exitError = 1

type options struct {
	root string
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(arguments []string, stdin io.Reader, stdout, stderr io.Writer) int {
	settings := options{}
	flags := flag.NewFlagSet("rootguard", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&settings.root, "root", "", "absolute trusted Git worktree root")
	if err := flags.Parse(arguments); err != nil {
		return exitError
	}
	if strings.TrimSpace(settings.root) == "" {
		if _, err := fmt.Fprintln(stderr, "rootguard: --root is required"); err != nil {
			return exitError
		}

		return exitError
	}
	commandArguments := flags.Args()
	if len(commandArguments) == 0 {
		if _, err := fmt.Fprintln(stderr, "rootguard: a child command is required after --"); err != nil {
			return exitError
		}

		return exitError
	}

	started := time.Now()
	before, err := rootguard.Capture(settings.root)
	if err != nil {
		if _, writeErr := fmt.Fprintf(stderr, "ROOT_PROTECTION_ARMING_FAILED (%v)\n", err); writeErr != nil {
			return exitError
		}

		return exitError
	}
	if _, err := fmt.Fprintf(
		stdout,
		"ROOT_PROTECTION_ARMED head=%s branch=%s workspaceFingerprint=%s untrackedEntries=%d untrackedRegularFiles=%d untrackedLogicalBytes=%d ignoredEntries=0 gitProcesses=%d snapshotMillis=%d\n",
		before.Head,
		before.Branch,
		before.WorkspaceFingerprint,
		before.Metrics.UntrackedEntries,
		before.Metrics.UntrackedRegularFiles,
		before.Metrics.UntrackedLogicalBytes,
		before.Metrics.GitProcesses,
		time.Since(started).Milliseconds(),
	); err != nil {
		return exitError
	}

	command := exec.Command(commandArguments[0], commandArguments[1:]...)
	command.Stdin = stdin
	command.Stdout = stdout
	command.Stderr = stderr
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	childErr := runChild(command)

	afterStarted := time.Now()
	after, snapshotErr := rootguard.Capture(settings.root)
	if snapshotErr != nil {
		if _, err := fmt.Fprintf(stderr, "ROOT_MUTATION_DETECTED (post-snapshot failed closed: %v)\n", snapshotErr); err != nil {
			return exitError
		}

		return exitError
	}
	compareErr := rootguard.Compare(before, after)
	if _, err := fmt.Fprintf(
		stdout,
		"ROOT_SNAPSHOT_CAPTURED position=after untrackedEntries=%d ignoredEntries=0 gitProcesses=%d snapshotMillis=%d\n",
		after.Metrics.UntrackedEntries,
		after.Metrics.GitProcesses,
		time.Since(afterStarted).Milliseconds(),
	); err != nil {
		return exitError
	}
	if compareErr != nil {
		if _, writeErr := fmt.Fprintf(stderr, "ROOT_MUTATION_DETECTED (%v)\n", compareErr); writeErr != nil {
			return exitError
		}

		return exitError
	}
	if _, err := fmt.Fprintln(stdout, "ROOT_UNCHANGED=PASS"); err != nil {
		return exitError
	}
	if childErr == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(childErr, &exitErr) && exitErr.ExitCode() >= 0 {
		return exitErr.ExitCode()
	}
	if _, err := fmt.Fprintf(stderr, "rootguard: child command failed: %v\n", childErr); err != nil {
		return exitError
	}

	return exitError
}

func runChild(command *exec.Cmd) error {
	if err := command.Start(); err != nil {
		return err
	}

	received := make(chan os.Signal, 1)
	signal.Notify(received, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(received)

	done := make(chan error, 1)
	go func() {
		done <- command.Wait()
	}()

	select {
	case err := <-done:
		return err
	case forwarded := <-received:
		if typed, ok := forwarded.(syscall.Signal); ok {
			_ = syscall.Kill(-command.Process.Pid, typed) // The child may have exited concurrently.
		} else {
			_ = command.Process.Signal(forwarded) // Best effort cancellation forwarding.
		}

		return <-done
	}
}
