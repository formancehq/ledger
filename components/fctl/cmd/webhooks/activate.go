package webhooks

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewActivateCommand() *cobra.Command {
	return fctl.NewCommand("activate <config-id>",
		fctl.WithShortDescription("Activate one config"),
		fctl.WithAliases("ac", "a"),
		fctl.WithConfirmFlag(),
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

			if !fctl.CheckStackApprobation(cmd, stack, "You are bout to activate a webhook") {
				return fctl.ErrMissingApproval
			}

			client, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return errors.Wrap(err, "creating stack client")
			}

			_, _, err = client.WebhooksApi.ActivateConfig(cmd.Context(), args[0]).Execute()
			if err != nil {
				return errors.Wrap(err, "activating config")
			}

			fctl.Success(cmd.OutOrStdout(), "Config activated successfully")
			return nil
		}),
	)
}
