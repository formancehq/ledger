package payments

import (
	"github.com/formancehq/fctl/cmd/payments/connectors"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return fctl.NewStackCommand("payments",
		fctl.WithShortDescription("Payments management"),
		fctl.WithChildCommands(
			connectors.NewConnectorsCommand(),
		),
	)
}
