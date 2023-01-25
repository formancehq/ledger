package users

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return fctl.NewCommand("users",
		fctl.WithAliases("u"),
		fctl.WithShortDescription("Users management"),
		fctl.WithChildCommands(
			NewListCommand(),
			NewShowCommand(),
		),
	)
}
