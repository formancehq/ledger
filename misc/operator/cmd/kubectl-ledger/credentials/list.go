package credentials

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/cmdutil"
)

func newListCommand(opts *cmdutil.Options) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List Credentials resources",
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

	spinner, _ := pterm.DefaultSpinner.Start("Fetching Credentials resources...")

	credentials, err := cmdutil.ListCredentials(ctx, crdClient)
	if err != nil {
		spinner.Fail("Failed to list Credentials resources")

		return fmt.Errorf("listing credentials: %w", err)
	}

	_ = spinner.Stop()

	switch opts.OutputFormat() {
	case "json":
		return cmdutil.OutputJSON(credentials)
	case "yaml":
		return cmdutil.OutputYAML(credentials)
	default:
		return renderCredentialsListTable(credentials)
	}
}

func renderCredentialsListTable(credentials *ledgerv1alpha1.CredentialsList) error {
	header := []string{"NAME", "KEY ID", "SCOPES", "MATCHED CLUSTERS", "PHASE", "AGE"}

	rows := make([][]string, 0, len(credentials.Items))
	for i := range credentials.Items {
		a := &credentials.Items[i]
		scopes := strings.Join(a.Spec.Scopes, ", ")
		matched := strconv.Itoa(len(a.Status.MatchedClusters))
		phase := cmdutil.PhaseColor(credentialsPhaseColor(a.Status.Phase))
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
		pterm.Info.Println("No Credentials resources found.")
		pterm.Println(pterm.Gray("Create one with: kubectl ledger credentials create"))

		return nil
	}

	pterm.Println()
	cmdutil.RenderTable(header, rows)

	return nil
}

func credentialsPhaseColor(phase string) string {
	switch phase {
	case "Ready":
		return "Running" // Reuse PhaseColor's "Running" for green
	case "Error":
		return "Degraded" // Reuse PhaseColor's "Degraded" for yellow
	default:
		return phase
	}
}
