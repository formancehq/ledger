package profile

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
)

// NewUseCommand returns the "profile use" command.
func NewUseCommand() *cobra.Command {
	return &cobra.Command{
		Use:               "use <name>",
		Short:             "Set the active connection profile",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runUse,
	}
}

func runUse(_ *cobra.Command, args []string) error {
	name := args[0]

	cfg, err := cmdutil.LoadConfig()
	if err != nil {
		return err
	}

	if cfg.Profiles == nil {
		return fmt.Errorf("profile %q not found (no profiles configured)", name)
	}

	if _, ok := cfg.Profiles[name]; !ok {
		return fmt.Errorf("profile %q not found", name)
	}

	cfg.ActiveProfile = name

	if err := cmdutil.SaveConfig(cfg); err != nil {
		return err
	}

	pterm.Success.Printfln("Active profile: %s (server: %s)", pterm.Bold.Sprint(name), cfg.Profiles[name].Server)

	return nil
}
