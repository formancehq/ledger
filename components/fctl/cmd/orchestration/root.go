package orchestration

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return fctl.NewStackCommand("orchestration",
		fctl.WithAliases("orch", "or"),
		fctl.WithShortDescription("Orchestration"),
		fctl.WithChildCommands(
			NewListWorkflowsCommand(),
			NewCreateWorkflowCommand(),
			NewShowWorkflowCommand(),
			NewRunWorkflowCommand(),
		),
	)
}
