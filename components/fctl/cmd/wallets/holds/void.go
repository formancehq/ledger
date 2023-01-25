package holds

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewVoidCommand() *cobra.Command {
	return fctl.NewCommand("void <hold-id>",
		fctl.WithShortDescription("Void a hold"),
		fctl.WithAliases("v"),
		fctl.WithArgs(cobra.ExactArgs(1)),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return errors.Wrap(err, "retrieving config")
			}

			organizationID, err := fctl.ResolveOrganizationID(cmd, cfg)
			if err != nil {
				return err
			}

			stack, err := fctl.ResolveStack(cmd, cfg, organizationID)
			if err != nil {
				return err
			}

			stackClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return errors.Wrap(err, "creating stack client")
			}

			_, err = stackClient.WalletsApi.VoidHold(cmd.Context(), args[0]).Execute()
			if err != nil {
				return errors.Wrap(err, "listing wallets")
			}

			fctl.Success(cmd.OutOrStdout(), "Hold '%s' voided!", args[0])

			return nil
		}),
	)
}
