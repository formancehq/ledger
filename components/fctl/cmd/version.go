package cmd

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var (
	Version   = "develop"
	BuildDate = "-"
	Commit    = "-"
)

func NewVersionCommand() *cobra.Command {
	return fctl.NewCommand("version",
		fctl.WithShortDescription("Get version"),
		fctl.WithArgs(cobra.ExactArgs(0)),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			tableData := pterm.TableData{}
			tableData = append(tableData, []string{pterm.LightCyan("Version"), Version})
			tableData = append(tableData, []string{pterm.LightCyan("Date"), BuildDate})
			tableData = append(tableData, []string{pterm.LightCyan("Commit"), Commit})
			return pterm.DefaultTable.
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render()
		}),
	)
}
