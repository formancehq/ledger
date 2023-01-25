package webhooks

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewDeleteCommand() *cobra.Command {
	return fctl.NewCommand("delete <config-id>",
		fctl.WithShortDescription("Delete a config"),
		fctl.WithConfirmFlag(),
		fctl.WithAliases("del"),
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

			if !fctl.CheckStackApprobation(cmd, stack, "You are about to delete a webhook") {
				return fctl.ErrMissingApproval
			}

			webhookClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return errors.Wrap(err, "creating stack client")
			}

			_, err = webhookClient.WebhooksApi.DeleteConfig(cmd.Context(), args[0]).Execute()
			if err != nil {
				return errors.Wrap(err, "deleting config")
			}

			fctl.Success(cmd.OutOrStdout(), "Config deleted successfully")
			return nil
		}),
	)
}
