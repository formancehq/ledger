package profiles

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pkg/errors"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewShowCommand() *cobra.Command {
	return fctl.NewCommand("show <name>",
		fctl.WithArgs(cobra.ExactArgs(1)),
		fctl.WithAliases("s"),
		fctl.WithShortDescription("Show profile"),
		fctl.WithValidArgsFunction(ProfileNamesAutoCompletion),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {

			config, err := fctl.GetConfig(cmd)
			if err != nil {
				return err
			}

			p := config.GetProfile(args[0])
			if p == nil {
				return errors.New("not found")
			}

			tableData := pterm.TableData{}
			tableData = append(tableData, []string{pterm.LightCyan("Membership URI"), p.GetMembershipURI()})
			tableData = append(tableData, []string{pterm.LightCyan("Default organization"), p.GetDefaultOrganization()})
			return pterm.DefaultTable.
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render()
		}),
	)
}
