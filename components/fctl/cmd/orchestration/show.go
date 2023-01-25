package orchestration

import (
	"fmt"
	"time"

	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pkg/errors"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func NewShowWorkflowCommand() *cobra.Command {
	return fctl.NewCommand("show <id>",
		fctl.WithShortDescription("Show a workflow"),
		fctl.WithAliases("s"),
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

			client, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return errors.Wrap(err, "creating stack client")
			}

			res, _, err := client.OrchestrationApi.
				GetFlow(cmd.Context(), args[0]).
				Execute()
			if err != nil {
				return errors.Wrap(err, "getting workflow")
			}

			fctl.Section.WithWriter(cmd.OutOrStdout()).Println("Information")
			tableData := pterm.TableData{}
			tableData = append(tableData, []string{pterm.LightCyan("ID"), res.Data.Id})
			tableData = append(tableData, []string{pterm.LightCyan("Created at"), res.Data.CreatedAt.Format(time.RFC3339)})
			tableData = append(tableData, []string{pterm.LightCyan("Updated at"), res.Data.UpdatedAt.Format(time.RFC3339)})

			if err := pterm.DefaultTable.
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render(); err != nil {
				return err
			}

			fmt.Fprintln(cmd.OutOrStdout())

			fctl.Section.WithWriter(cmd.OutOrStdout()).Println("Configuration")
			configAsBytes, err := yaml.Marshal(res.Data.Config)
			if err != nil {
				panic(err)
			}
			fmt.Fprintln(cmd.OutOrStdout(), string(configAsBytes))

			return nil
		}),
	)
}
