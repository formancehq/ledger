package orchestration

import (
	"time"

	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewListWorkflowsCommand() *cobra.Command {
	return fctl.NewCommand("list",
		fctl.WithShortDescription("List all workflows"),
		fctl.WithAliases("ls", "l"),
		fctl.WithArgs(cobra.ExactArgs(0)),
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

			client, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return errors.Wrap(err, "creating stack client")
			}

			res, _, err := client.OrchestrationApi.ListFlows(cmd.Context()).Execute()
			if err != nil {
				return errors.Wrap(err, "listing workflows")
			}

			if len(res.Data) == 0 {
				fctl.Println("No workflows found.")
				return nil
			}

			if err := pterm.DefaultTable.
				WithHasHeader(true).
				WithWriter(cmd.OutOrStdout()).
				WithData(
					fctl.Prepend(
						fctl.Map(res.Data,
							func(src formance.Workflow) []string {
								return []string{
									src.Id,
									src.CreatedAt.Format(time.RFC3339),
									src.UpdatedAt.Format(time.RFC3339),
								}
							}),
						[]string{"ID", "Created at", "Updated at"},
					),
				).Render(); err != nil {
				return errors.Wrap(err, "rendering table")
			}

			return nil
		}),
	)
}
