package me

import (
	"errors"

	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewInfoCommand() *cobra.Command {
	return fctl.NewCommand("info",
		fctl.WithAliases("i", "in"),
		fctl.WithShortDescription("Display user information"),
		fctl.WithArgs(cobra.ExactArgs(0)),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {

			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return err
			}

			profile := fctl.GetCurrentProfile(cmd, cfg)
			if !profile.IsConnected() {
				return errors.New("Not logged. Use 'login' command before.")
			}

			userInfo, err := profile.GetUserInfo(cmd)
			if err != nil {
				return err
			}

			tableData := pterm.TableData{}
			tableData = append(tableData, []string{pterm.LightCyan("Subject"), userInfo.GetSubject()})
			tableData = append(tableData, []string{pterm.LightCyan("Email"), userInfo.GetEmail()})

			return pterm.DefaultTable.
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render()
		}),
	)
}
