package stack

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return fctl.NewMembershipCommand("stack",
		fctl.WithShortDescription("Manage your stack"),
		fctl.WithAliases("stack", "stacks", "st"),
		fctl.WithChildCommands(
			NewCreateCommand(),
			NewListCommand(),
			NewDeleteCommand(),
			NewShowCommand(),
		),
	)
}
