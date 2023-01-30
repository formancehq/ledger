package billing

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return fctl.NewStackCommand("billing",
		fctl.WithAliases("bil", "b"),
		fctl.WithShortDescription("Billing management"),
		fctl.WithChildCommands(
			NewPortalCommand(),
			NewSetupCommand(),
		),
	)
}
