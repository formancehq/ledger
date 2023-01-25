package me

import (
	"github.com/formancehq/fctl/cmd/cloud/me/invitations"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return fctl.NewCommand("me",
		fctl.WithShortDescription("Current use management"),
		fctl.WithChildCommands(
			invitations.NewCommand(),
			NewInfoCommand(),
		),
	)
}
