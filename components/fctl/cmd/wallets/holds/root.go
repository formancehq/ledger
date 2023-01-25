package holds

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return fctl.NewCommand("holds",
		fctl.WithAliases("h", "hold"),
		fctl.WithShortDescription("Wallets holds management"),
		fctl.WithChildCommands(
			NewListCommand(),
			NewVoidCommand(),
			NewConfirmCommand(),
			NewShowCommand(),
		),
	)
}
