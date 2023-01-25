package profiles

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return fctl.NewCommand("profiles",
		fctl.WithAliases("p", "prof"),
		fctl.WithShortDescription("Profiles management"),
		fctl.WithChildCommands(
			NewDeleteCommand(),
			NewListCommand(),
			NewRenameCommand(),
			NewShowCommand(),
			NewUseCommand(),
			NewSetDefaultOrganizationCommand(),
		),
	)
}
