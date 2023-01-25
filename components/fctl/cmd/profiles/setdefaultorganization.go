package profiles

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewSetDefaultOrganizationCommand() *cobra.Command {
	return fctl.NewCommand("set-default-organization <organization-id>",
		fctl.WithArgs(cobra.ExactArgs(1)),
		fctl.WithAliases("sdo"),
		fctl.WithShortDescription("Set default organization"),
		fctl.WithValidArgsFunction(ProfileNamesAutoCompletion),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return err
			}

			fctl.GetCurrentProfile(cmd, cfg).SetDefaultOrganization(args[0])

			if err := cfg.Persist(); err != nil {
				return errors.Wrap(err, "Updating config")
			}
			fctl.Success(cmd.OutOrStdout(), "Default organization updated!")
			return nil
		}),
	)
}
