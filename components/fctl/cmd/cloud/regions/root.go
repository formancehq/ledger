package regions

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return fctl.NewCommand("regions",
		fctl.WithAliases("region", "reg"),
		fctl.WithShortDescription("Regions management"),
		fctl.WithChildCommands(
			NewListCommand(),
			NewShowCommand(),
		),
	)
}
