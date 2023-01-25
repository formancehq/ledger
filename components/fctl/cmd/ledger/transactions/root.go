package transactions

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewLedgerTransactionsCommand() *cobra.Command {
	return fctl.NewCommand("transactions",
		fctl.WithAliases("t", "txs", "tx"),
		fctl.WithShortDescription("Transactions management"),
		fctl.WithChildCommands(
			NewListCommand(),
			NewCommand(),
			NewRevertCommand(),
			NewShowCommand(),
			NewSetMetadataCommand(),
		),
	)
}
