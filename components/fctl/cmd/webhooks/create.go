package webhooks

import (
	"net/url"

	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewCreateCommand() *cobra.Command {
	const (
		secretFlag = "secret"
	)
	return fctl.NewCommand("create <endpoint> [<event-type>...]",
		fctl.WithShortDescription("Create a new config. At least one event type is required."),
		fctl.WithAliases("cr"),
		fctl.WithConfirmFlag(),
		fctl.WithArgs(cobra.MinimumNArgs(2)),
		fctl.WithStringFlag(secretFlag, "", "Bring your own webhooks signing secret. If not passed or empty, a secret is automatically generated. The format is a string of bytes of size 24, base64 encoded. (larger size after encoding)"),
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

			if !fctl.CheckStackApprobation(cmd, stack, "You are about to create a webhook") {
				return fctl.ErrMissingApproval
			}

			client, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return errors.Wrap(err, "creating stack client")
			}

			if _, err := url.Parse(args[0]); err != nil {
				return errors.Wrap(err, "invalid endpoint URL")
			}

			secret := fctl.GetString(cmd, secretFlag)

			res, _, err := client.WebhooksApi.InsertConfig(cmd.Context()).
				ConfigUser(formance.ConfigUser{
					Endpoint:   args[0],
					EventTypes: args[1:],
					Secret:     &secret,
				}).Execute()
			if err != nil {
				return errors.Wrap(err, "inserting config")
			}

			fctl.Success(cmd.OutOrStdout(),
				"Config created successfully with ID: %s", *res.Data.Id)
			return nil
		}),
	)
}
