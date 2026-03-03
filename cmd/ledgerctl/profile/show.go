package profile

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewShowCommand returns the "profile show" command.
func NewShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show [name]",
		Aliases: []string{"get", "describe"},
		Short:   "Show details of a connection profile",
		Long:    "Show details of a connection profile. Defaults to the active profile if no name is given.",
		Args:    cobra.MaximumNArgs(1),
		RunE:    runShow,
	}
}

func runShow(cmd *cobra.Command, args []string) error {
	cfg, err := cmdutil.LoadConfig()
	if err != nil {
		return err
	}

	var name string
	if len(args) > 0 {
		name = args[0]
	} else {
		name = cfg.ActiveProfile
	}

	if name == "" {
		return fmt.Errorf("no profile specified and no active profile set")
	}

	if cfg.Profiles == nil {
		return fmt.Errorf("profile %q not found", name)
	}
	p, ok := cfg.Profiles[name]
	if !ok {
		return fmt.Errorf("profile %q not found", name)
	}

	active := ""
	if name == cfg.ActiveProfile {
		active = " (active)"
	}

	pterm.DefaultSection.Printfln("Profile: %s%s", name, active)

	insecure := "false"
	if p.Insecure {
		insecure = "true"
	}

	tlsCaCert := "(none)"
	if p.TLSCaCert != "" {
		tlsCaCert = p.TLSCaCert
	}

	kr := cmdutil.GetKeyring(cmd)
	authStatus := pterm.Yellow("no token stored")
	if cmdutil.HasStoredToken(kr, p.Server) {
		authStatus = pterm.Green("token stored in keychain")
	}

	data := pterm.TableData{
		{"Field", "Value"},
		{"Server", p.Server},
		{"Insecure", insecure},
		{"TLS CA Cert", tlsCaCert},
		{"Auth Status", authStatus},
	}

	return pterm.DefaultTable.WithHasHeader().WithData(data).Render()
}
