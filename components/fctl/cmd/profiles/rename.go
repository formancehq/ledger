package profiles

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewRenameCommand() *cobra.Command {
	return fctl.NewCommand("rename <old-name> <new-name>",
		fctl.WithArgs(cobra.ExactArgs(2)),
		fctl.WithShortDescription("Rename a profile"),
		fctl.WithValidArgsFunction(ProfileNamesAutoCompletion),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			oldName := args[0]
			newName := args[1]

			config, err := fctl.GetConfig(cmd)
			if err != nil {
				return err
			}

			p := config.GetProfile(oldName)
			if p == nil {
				return errors.New("profile not found")
			}

			if err := config.DeleteProfile(oldName); err != nil {
				return err
			}
			if config.GetCurrentProfileName() == oldName {
				config.SetCurrentProfile(newName, p)
			} else {
				config.SetProfile(newName, p)
			}

			if err := config.Persist(); err != nil {
				return errors.Wrap(config.Persist(), "Updating config")
			}
			return nil
		}),
	)
}
