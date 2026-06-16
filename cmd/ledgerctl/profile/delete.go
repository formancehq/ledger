package profile

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
)

// NewDeleteCommand returns the "profile delete" command.
func NewDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "delete <name>",
		Aliases:           []string{"rm", "remove"},
		Short:             "Delete a connection profile",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runDelete,
	}

	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")

	return cmd
}

func runDelete(cmd *cobra.Command, args []string) error {
	name := args[0]
	skipConfirm, _ := cmd.Flags().GetBool("yes")

	cfg, err := cmdutil.LoadConfig()
	if err != nil {
		return err
	}

	if cfg.Profiles == nil {
		return fmt.Errorf("profile %q not found", name)
	}

	if _, ok := cfg.Profiles[name]; !ok {
		return fmt.Errorf("profile %q not found", name)
	}

	if !skipConfirm {
		pterm.Warning.Printfln("Delete profile %q?", name)

		result, _ := pterm.DefaultInteractiveConfirm.Show()
		if !result {
			pterm.Info.Println("Cancelled.")

			return nil
		}
	}

	delete(cfg.Profiles, name)

	// Clear active profile if it was the deleted one.
	if cfg.ActiveProfile == name {
		cfg.ActiveProfile = ""
	}

	if err := cmdutil.SaveConfig(cfg); err != nil {
		return err
	}

	pterm.Success.Printfln("Profile %s deleted", pterm.Bold.Sprint(name))

	if cfg.ActiveProfile == "" && len(cfg.Profiles) > 0 {
		pterm.Info.Println("No active profile. Use: ledgerctl profile use <name>")
	}

	return nil
}
