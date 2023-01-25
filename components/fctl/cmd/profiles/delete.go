package profiles

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewDeleteCommand() *cobra.Command {
	return fctl.NewCommand("delete <name>",
		fctl.WithArgs(cobra.ExactArgs(1)),
		fctl.WithShortDescription("Delete a profile"),
		fctl.WithValidArgsFunction(ProfileNamesAutoCompletion),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {

			config, err := fctl.GetConfig(cmd)
			if err != nil {
				return err
			}
			if err := config.DeleteProfile(args[0]); err != nil {
				return err
			}

			if err := config.Persist(); err != nil {
				return errors.Wrap(err, "updating config")
			}
			fctl.Success(cmd.OutOrStdout(), "Profile deleted!")
			return nil
		}),
	)
}
