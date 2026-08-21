package ledgers

import "github.com/spf13/cobra"

func NewHistoricalBalancesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "historical-balances",
		Aliases:           []string{"balance-history"},
		Short:             "Configure and inspect historical balances",
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
	}
	cmd.AddCommand(newConfigureHistoricalBalancesCommand(true))
	cmd.AddCommand(newConfigureHistoricalBalancesCommand(false))
	cmd.AddCommand(newHistoricalBalancesStatusCommand())

	return cmd
}
