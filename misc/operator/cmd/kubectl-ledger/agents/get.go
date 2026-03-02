package agents

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
		Short: "Show details of a LedgerClusterAgent resource",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGet(cmd, opts, args)
		},
	}
}

func runGet(cmd *cobra.Command, opts *cmdutil.Options, args []string) error {
	ctx := cmd.Context()

	name, err := cmdutil.ResolveLedgerClusterAgentName(ctx, opts, args)
	if err != nil {
		return err
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	agent, err := cmdutil.GetLedgerClusterAgent(ctx, crdClient, name)
	if err != nil {
		return fmt.Errorf("getting agent %q: %w", name, err)
	}

	switch opts.OutputFormat() {
	case "json":
		return cmdutil.OutputJSON(agent)
	case "yaml":
		return cmdutil.OutputYAML(agent)
	default:
		return renderAgentDetails(agent)
	}
}

func renderAgentDetails(agent *ledgerv1alpha1.LedgerClusterAgent) error {
	pterm.Println()
	pterm.DefaultSection.Printfln("LedgerClusterAgent: %s", pterm.Cyan(agent.Name))

	// Show spec and status as YAML for readability.
	type agentView struct {
		Spec   ledgerv1alpha1.LedgerClusterAgentSpec   `json:"spec"`
		Status ledgerv1alpha1.LedgerClusterAgentStatus `json:"status"`
	}

	b, err := yaml.Marshal(agentView{Spec: agent.Spec, Status: agent.Status})
	if err != nil {
		return fmt.Errorf("marshaling agent: %w", err)
	}
	fmt.Print(string(b))
	return nil
}
