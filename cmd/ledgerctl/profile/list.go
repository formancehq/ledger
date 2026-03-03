package profile

import (
	"sort"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewListCommand returns the "profile list" command.
func NewListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "l"},
		Short:   "List all connection profiles",
		RunE:    runList,
	}
}

func runList(_ *cobra.Command, _ []string) error {
	cfg, err := cmdutil.LoadConfig()
	if err != nil {
		return err
	}

	if len(cfg.Profiles) == 0 {
		pterm.Info.Println("No profiles configured. Create one with: ledgerctl profile create <name> --server <addr>")
		return nil
	}

	// Sort names for stable output.
	names := make([]string, 0, len(cfg.Profiles))
	for name := range cfg.Profiles {
		names = append(names, name)
	}
	sort.Strings(names)

	data := pterm.TableData{{"", "NAME", "SERVER", "INSECURE", "TLS CA CERT"}}
	for _, name := range names {
		p := cfg.Profiles[name]
		marker := " "
		if name == cfg.ActiveProfile {
			marker = "*"
		}
		insecure := ""
		if p.Insecure {
			insecure = "true"
		}
		data = append(data, []string{marker, name, p.Server, insecure, p.TLSCaCert})
	}

	return pterm.DefaultTable.WithHasHeader().WithData(data).Render()
}
