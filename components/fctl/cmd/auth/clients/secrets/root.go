package secrets

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return fctl.NewCommand("secrets",
		fctl.WithAliases("sec"),
		fctl.WithShortDescription("Secrets management"),
		fctl.WithChildCommands(
			NewCreateCommand(),
			NewDeleteCommand(),
		),
	)
}
