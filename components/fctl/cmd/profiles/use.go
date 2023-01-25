package profiles

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func ProfileNamesAutoCompletion(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	ret, err := fctl.ListProfiles(cmd, toComplete)
	if err != nil {
		return []string{}, cobra.ShellCompDirectiveError
	}

	return ret, cobra.ShellCompDirectiveDefault
}

func NewUseCommand() *cobra.Command {
	return fctl.NewCommand("use <name>",
		fctl.WithAliases("u"),
		fctl.WithArgs(cobra.ExactArgs(1)),
		fctl.WithShortDescription("Use profile"),
		fctl.WithValidArgsFunction(ProfileNamesAutoCompletion),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			config, err := fctl.GetConfig(cmd)
			if err != nil {
				return err
			}

			config.SetCurrentProfileName(args[0])
			if err := config.Persist(); err != nil {
				return errors.Wrap(err, "Updating config")
			}
			fctl.Success(cmd.OutOrStdout(), "Selected profile updated!")
			return nil
		}),
	)
}
