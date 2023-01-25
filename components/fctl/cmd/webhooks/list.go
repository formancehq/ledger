package webhooks

import (
	"strings"
	"time"

	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewListCommand() *cobra.Command {
	return fctl.NewCommand("list",
		fctl.WithShortDescription("List all configs"),
		fctl.WithAliases("ls", "l"),
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

			webhookClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return errors.Wrap(err, "creating stack client")
			}

			res, _, err := webhookClient.WebhooksApi.GetManyConfigs(cmd.Context()).Execute()
			if err != nil {
				return errors.Wrap(err, "listing all configs")
			}

			if err := pterm.DefaultTable.
				WithHasHeader(true).
				WithWriter(cmd.OutOrStdout()).
				WithData(
					fctl.Prepend(
						fctl.Map(res.Cursor.Data,
							func(src formance.WebhooksConfig) []string {
								return []string{
									*src.Id,
									src.CreatedAt.Format(time.RFC3339),
									fctl.StringPointerToString(src.Secret),
									*src.Endpoint,
									fctl.BoolPointerToString(src.Active),
									strings.Join(src.EventTypes, ","),
								}
							}),
						[]string{"ID", "Created at", "Secret", "Endpoint", "Active", "Event types"},
					),
				).Render(); err != nil {
				return errors.Wrap(err, "rendering table")
			}

			return nil
		}),
	)
}
