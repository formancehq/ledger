package webhooks

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return fctl.NewCommand("webhooks",
		fctl.WithAliases("web", "wh"),
		fctl.WithShortDescription("Webhooks management"),
		fctl.WithChildCommands(
			NewCreateCommand(),
			NewListCommand(),
			NewDeactivateCommand(),
			NewActivateCommand(),
			NewDeleteCommand(),
			NewChangeSecretCommand(),
		),
	)
}
