package invitations

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return fctl.NewStackCommand("invitations",
		fctl.WithAliases("invit", "inv", "i"),
		fctl.WithShortDescription("Invitations management"),
		fctl.WithChildCommands(
			NewSendCommand(),
			NewListCommand(),
		),
	)
}
