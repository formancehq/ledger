package transactions

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return fctl.NewCommand("transactions",
		fctl.WithAliases("transaction", "tx", "txs"),
		fctl.WithShortDescription("Wallet transactions"),
		fctl.WithChildCommands(
			NewListCommand(),
		),
	)
}
