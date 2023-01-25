package cloud

import (
	"fmt"

	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewGeneratePersonalTokenCommand() *cobra.Command {
	return fctl.NewCommand("generate-personal-token",
		fctl.WithDescription("Generate a personal bearer token"),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return err
			}
			profile := fctl.GetCurrentProfile(cmd, cfg)

			organizationID, err := fctl.ResolveOrganizationID(cmd, cfg)
			if err != nil {
				return err
			}

			stack, err := fctl.ResolveStack(cmd, cfg, organizationID)
			if err != nil {
				return err
			}

			token, err := profile.GetStackToken(cmd.Context(), fctl.GetHttpClient(cmd), stack)
			if err != nil {
				return err
			}

			fmt.Fprintln(cmd.OutOrStdout(), token)
			return nil
		}),
	)
}
