package agents

import (
	"fmt"
	"strings"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

func newListCommand(opts *cmdutil.Options) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List LedgerClusterAgent resources",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runList(cmd, opts)
		},
	}
}

func runList(cmd *cobra.Command, opts *cmdutil.Options) error {
	ctx := cmd.Context()

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	spinner, _ := pterm.DefaultSpinner.Start("Fetching LedgerClusterAgent resources...")

	agents, err := cmdutil.ListLedgerClusterAgents(ctx, crdClient)
	if err != nil {
		spinner.Fail("Failed to list LedgerClusterAgent resources")
		return fmt.Errorf("listing agents: %w", err)
	}

	_ = spinner.Stop()

	switch opts.OutputFormat() {
	case "json":
		return cmdutil.OutputJSON(agents)
	case "yaml":
		return cmdutil.OutputYAML(agents)
	default:
		return renderAgentListTable(agents)
	}
}

func renderAgentListTable(agents *ledgerv1alpha1.LedgerClusterAgentList) error {
	header := []string{"NAME", "KEY ID", "SCOPES", "MATCHED SERVICES", "PHASE", "AGE"}

	rows := make([][]string, 0, len(agents.Items))
	for i := range agents.Items {
		a := &agents.Items[i]
		scopes := strings.Join(a.Spec.Scopes, ", ")
		matched := fmt.Sprintf("%d", len(a.Status.MatchedServices))
		phase := cmdutil.PhaseColor(agentPhaseColor(a.Status.Phase))
		age := cmdutil.FormatAge(time.Since(a.CreationTimestamp.Time))
		keyID := a.Status.KeyID
		if keyID == "" {
			keyID = pterm.Gray("<pending>")
		}

		rows = append(rows, []string{
			pterm.Cyan(a.Name),
			keyID,
			scopes,
			matched,
			phase,
			age,
		})
	}

	if len(rows) == 0 {
		pterm.Info.Println("No LedgerClusterAgent resources found.")
		pterm.Println(pterm.Gray("Create one with: kubectl ledger agents create"))
		return nil
	}

	pterm.Println()
	cmdutil.RenderTable(header, rows)
	return nil
}

func agentPhaseColor(phase string) string {
	switch phase {
	case "Ready":
		return "Running" // Reuse PhaseColor's "Running" for green
	case "Error":
		return "Degraded" // Reuse PhaseColor's "Degraded" for yellow
	default:
		return phase
	}
}
