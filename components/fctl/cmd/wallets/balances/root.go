package balances

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return fctl.NewCommand("balances",
		fctl.WithAliases("balance", "bls", "bal"),
		fctl.WithShortDescription("Wallet balances"),
		fctl.WithChildCommands(
			NewListCommand(),
			NewShowCommand(),
			NewCreateCommand(),
		),
	)
}
