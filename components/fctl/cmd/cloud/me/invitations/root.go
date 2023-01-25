package invitations

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return fctl.NewCommand("invitations",
		fctl.WithShortDescription("Invitations management"),
		fctl.WithAliases("invit", "i"),
		fctl.WithChildCommands(
			NewListCommand(),
			NewAcceptCommand(),
			NewDeclineCommand(),
		),
	)
}
