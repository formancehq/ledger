package provision

import (
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/pkg/scenario"
)

func NewListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List available provisioning scenarios",
		Args:    cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			names := scenario.List()
			if len(names) == 0 {
				pterm.Info.Println("No scenarios registered.")

				return
			}

			tableData := pterm.TableData{{"NAME"}}
			for _, name := range names {
				tableData = append(tableData, []string{name})
			}
			_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
		},
	}
}
