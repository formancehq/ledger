package orchestration

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func NewCreateWorkflowCommand() *cobra.Command {
	return fctl.NewCommand("create <file>|-",
		fctl.WithShortDescription("Create a workflow"),
		fctl.WithAliases("cr", "c"),
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

			script, err := fctl.ReadFile(cmd, stack, args[0])
			if err != nil {
				return err
			}

			config := formance.WorkflowConfig{}
			if err := yaml.Unmarshal([]byte(script), &config); err != nil {
				return err
			}

			res, _, err := client.OrchestrationApi.
				CreateWorkflow(cmd.Context()).
				Body(config).
				Execute()
			if err != nil {
				return errors.Wrap(err, "listing workflows")
			}

			fctl.Success(cmd.OutOrStdout(), "Workflow created with ID: %s", res.Data.Id)

			return nil
		}),
	)
}
