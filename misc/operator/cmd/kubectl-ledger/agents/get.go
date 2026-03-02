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

	// Overview section
	phase := cmdutil.PhaseColor(agentPhaseColor(agent.Status.Phase))
	keyID := agent.Status.KeyID
	if keyID == "" {
		keyID = pterm.Gray("<pending>")
	}
	secretRef := pterm.Gray("<none>")
	if agent.Status.SecretRef.Name != "" {
		secretRef = fmt.Sprintf("%s/%s", agent.Status.SecretRef.Namespace, agent.Status.SecretRef.Name)
	}
	scopes := strings.Join(agent.Spec.Scopes, ", ")
	if scopes == "" {
		scopes = pterm.Gray("<none>")
	}

	pterm.DefaultSection.Println("Overview")
	cmdutil.RenderTable(
		[]string{"FIELD", "VALUE"},
		[][]string{
			{"Name", pterm.Cyan(agent.Name)},
			{"Phase", phase},
			{"Key ID", keyID},
			{"Secret", secretRef},
			{"Scopes", scopes},
			{"Matched Services", fmt.Sprintf("%d", len(agent.Status.MatchedServices))},
			{"Age", cmdutil.FormatAge(time.Since(agent.CreationTimestamp.Time))},
		},
	)

	// Matched services section
	if len(agent.Status.MatchedServices) > 0 {
		pterm.DefaultSection.Println("Matched Services")
		svcRows := make([][]string, 0, len(agent.Status.MatchedServices))
		for i := range agent.Status.MatchedServices {
			ms := &agent.Status.MatchedServices[i]
			svcRows = append(svcRows, []string{ms.Namespace, ms.Name})
		}
		cmdutil.RenderTable([]string{"NAMESPACE", "NAME"}, svcRows)
	}

	// Conditions section
	if len(agent.Status.Conditions) > 0 {
		pterm.DefaultSection.Println("Conditions")
		condRows := make([][]string, 0, len(agent.Status.Conditions))
		for i := range agent.Status.Conditions {
			c := &agent.Status.Conditions[i]
			condRows = append(condRows, []string{
				c.Type,
				string(c.Status),
				c.Reason,
				c.Message,
				cmdutil.FormatAge(time.Since(c.LastTransitionTime.Time)),
			})
		}
		cmdutil.RenderTable([]string{"TYPE", "STATUS", "REASON", "MESSAGE", "AGE"}, condRows)
	}

	return nil
}
