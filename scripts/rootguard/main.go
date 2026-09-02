package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
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
		"ROOT_PROTECTION_ARMED head=%s branch=%s statusSha256=%s workspaceFingerprint=%s entries=%d regularFiles=%d logicalBytes=%d ignoredEntries=%d ignoredRegularFiles=%d ignoredLogicalBytes=%d gitProcesses=%d snapshotMillis=%d\n",
		before.Head,
		before.Branch,
		before.StatusSHA256(),
		before.WorkspaceFingerprint,
		before.Metrics.Entries,
		before.Metrics.RegularFiles,
		before.Metrics.LogicalBytes,
		before.Metrics.IgnoredEntries,
		before.Metrics.IgnoredRegularFiles,
		before.Metrics.IgnoredLogicalBytes,
		before.Metrics.GitProcesses,
		time.Since(started).Milliseconds(),
	); err != nil {
		return exitError
	}

	command := exec.Command(commandArguments[0], commandArguments[1:]...)
	command.Stdin = stdin
	command.Stdout = stdout
	command.Stderr = stderr
	childErr := command.Run()

	after, snapshotErr := rootguard.Capture(settings.root)
	if snapshotErr != nil {
		if _, err := fmt.Fprintf(stderr, "ROOT_MUTATION_DETECTED (post-snapshot failed closed: %v)\n", snapshotErr); err != nil {
			return exitError
		}

		return exitError
	}
	if err := rootguard.Compare(before, after); err != nil {
		if _, writeErr := fmt.Fprintf(stderr, "ROOT_MUTATION_DETECTED (%v)\n", err); writeErr != nil {
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
