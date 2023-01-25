package clients

import (
	"github.com/formancehq/fctl/cmd/auth/clients/secrets"
	"github.com/formancehq/fctl/cmd/auth/users"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return fctl.NewCommand("clients",
		fctl.WithAliases("client", "c"),
		fctl.WithShortDescription("Clients management"),
		fctl.WithChildCommands(
			NewListCommand(),
			NewCreateCommand(),
			NewDeleteCommand(),
			NewUpdateCommand(),
			NewShowCommand(),
			secrets.NewCommand(),
			users.NewCommand(),
		),
	)
}
