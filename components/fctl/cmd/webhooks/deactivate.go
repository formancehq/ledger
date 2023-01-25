package webhooks

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewDeactivateCommand() *cobra.Command {
	return fctl.NewCommand("deactivate <config-id>",
		fctl.WithShortDescription("Deactivate one config"),
		fctl.WithConfirmFlag(),
		fctl.WithAliases("deac"),
		fctl.WithArgs(cobra.ExactArgs(1)),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return errors.Wrap(err, "fctl.GetConfig")
			}

			organizationID, err := fctl.ResolveOrganizationID(cmd, cfg)
			if err != nil {
				return err
			}

			stack, err := fctl.ResolveStack(cmd, cfg, organizationID)
			if err != nil {
				return err
			}

			if !fctl.CheckStackApprobation(cmd, stack, "You are about to deactivate a webhook") {
				return fctl.ErrMissingApproval
			}

			client, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return errors.Wrap(err, "creating stack client")
			}

			_, _, err = client.WebhooksApi.DeactivateConfig(cmd.Context(), args[0]).Execute()
			if err != nil {
				return errors.Wrap(err, "deactivating config")
			}

			fctl.Success(cmd.OutOrStdout(), "Config deactivated successfully")
			return nil
		}),
	)
}
