package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// TestLedgerFlagCompletionRegistered asserts that every command exposing a
// --ledger flag (locally or via the inherited persistent flag) has the
// ledger-name shell completion wired, so pressing TAB suggests ledgers instead
// of falling back to file completion.
func TestLedgerFlagCompletionRegistered(t *testing.T) {
	t.Parallel()

	root := newRootCommand()

	var withLedgerFlag int

	var walk func(cmd *cobra.Command)
	walk = func(cmd *cobra.Command) {
		if cmd.Flag("ledger") != nil {
			withLedgerFlag++

			_, ok := cmd.GetFlagCompletionFunc("ledger")
			require.Truef(t, ok, "command %q exposes --ledger without completion", cmd.CommandPath())
		}

		for _, sub := range cmd.Commands() {
			walk(sub)
		}
	}
	walk(root)

	// Guard against the walk silently matching nothing (e.g. the flag being
	// renamed): the suite must actually exercise the registration path.
	require.NotZero(t, withLedgerFlag, "expected at least one command with a --ledger flag")
}
