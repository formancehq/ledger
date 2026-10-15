// Command lintproto runs `buf lint` on misc/proto with the repository's check
// plugin (tools/buf-plugin-ledger) built on the fly.
package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/formancehq/ledger/v3/scripts/internal/bufplugin"
)

func main() {
	if len(os.Args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: lintproto")
		os.Exit(2)
	}

	if err := run(); err != nil {
		if _, ok := errors.AsType[*exec.ExitError](err); ok {
			fmt.Fprintln(os.Stderr, "lint-proto: FAIL")
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "lint-proto: %v\nlint-proto: FAIL\n", err)
		os.Exit(1)
	}

	fmt.Println("lint-proto: PASS")
}

func run() error {
	root, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return fmt.Errorf("locating repository root: %w", err)
	}
	repoRoot := strings.TrimSpace(string(root))

	binDir, err := os.MkdirTemp("", "lintproto-")
	if err != nil {
		return fmt.Errorf("creating temporary directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(binDir) }() // best-effort cleanup of the built plugin

	env, err := bufplugin.Environment(repoRoot, binDir)
	if err != nil {
		return err
	}

	cmd := exec.Command("buf", "lint", "misc/proto")
	cmd.Dir = repoRoot
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}
