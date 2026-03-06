package defaults

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

func newGetCommand(opts *cmdutil.Options) *cobra.Command {
	return &cobra.Command{
		Use:   "get [name]",
		Short: "Show details of a LedgerDefaults resource",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGet(cmd, opts, args)
		},
	}
}

func runGet(cmd *cobra.Command, opts *cmdutil.Options, args []string) error {
	ctx := cmd.Context()

	name, err := cmdutil.ResolveLedgerDefaultsName(ctx, opts, args)
	if err != nil {
		return err
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	defaults, err := cmdutil.GetLedgerDefaults(ctx, crdClient, name)
	if err != nil {
		return fmt.Errorf("getting ledger defaults %q: %w", name, err)
	}

	switch opts.OutputFormat() {
	case "json":
		return cmdutil.OutputJSON(defaults)
	case "yaml":
		return cmdutil.OutputYAML(defaults)
	default:
		return renderGetDetails(defaults)
	}
}

func renderGetDetails(defaults *ledgerv1alpha1.LedgerDefaults) error {
	pterm.Println()
	pterm.DefaultSection.Printfln("LedgerDefaults: %s", pterm.Cyan(defaults.Name))

	// Show spec as YAML for readability.
	b, err := yaml.Marshal(defaults.Spec)
	if err != nil {
		return fmt.Errorf("marshaling spec: %w", err)
	}
	fmt.Print(string(b))

	return nil
}
