package cmdutil

import "github.com/spf13/cobra"

// RegisterEnumCompletion wires shell completion for a flag that accepts a fixed
// set of values. The candidates are offered verbatim and file completion is
// disabled so shells only suggest the enum values.
//
// RegisterFlagCompletionFunc only returns an error when flagName is not defined
// on cmd (a programming error caught during development) or already has a
// completion registered; both are intentionally ignored here, matching the
// existing --profile/--ledger call sites in main.go.
func RegisterEnumCompletion(cmd *cobra.Command, flagName string, values ...string) {
	_ = cmd.RegisterFlagCompletionFunc(flagName, cobra.FixedCompletions(values, cobra.ShellCompDirectiveNoFileComp))
}
