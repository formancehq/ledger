package list

import (
	"fmt"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/cmdutil"
)

type listFlags struct {
	allNamespaces bool
}

// NewCommand returns the "list" command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	var f listFlags

	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List Cluster deployments",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runList(cmd, opts, &f)
		},
	}

	cmd.Flags().BoolVarP(&f.allNamespaces, "all-namespaces", "A", false, "List across all namespaces")
	cmd.Flags().BoolVarP(&f.allNamespaces, "all", "a", false, "Alias for --all-namespaces")

	return cmd
}

func runList(cmd *cobra.Command, opts *cmdutil.Options, f *listFlags) error {
	ctx := cmd.Context()

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	ns := ""
	if !f.allNamespaces {
		ns, err = opts.ResolvedNamespace()
		if err != nil {
			return fmt.Errorf("resolving namespace: %w", err)
		}
	}

	spinner, _ := pterm.DefaultSpinner.Start("Fetching Cluster resources...")

	ledgers, err := cmdutil.ListClusters(ctx, crdClient, ns)
	if err != nil {
		spinner.Fail("Failed to list Cluster resources")

		return fmt.Errorf("listing ledgers: %w", err)
	}

	_ = spinner.Stop()

	switch opts.OutputFormat() {
	case "json":
		return cmdutil.OutputJSON(ledgers)
	case "yaml":
		return cmdutil.OutputYAML(ledgers)
	default:
		return renderTable(ledgers, f.allNamespaces)
	}
}

func renderTable(ledgers *ledgerv1alpha1.ClusterList, showNamespace bool) error {
	header := []string{"NAME", "REPLICAS", "PHASE", "IMAGE", "URL", "AGE"}
	if showNamespace {
		header = append([]string{"NAMESPACE"}, header...)
	}

	rows := make([][]string, 0, len(ledgers.Items))
	for i := range ledgers.Items {
		l := &ledgers.Items[i]
		image := cmdutil.FormatImage(l.Spec.Image)
		age := cmdutil.FormatAge(time.Since(l.CreationTimestamp.Time))
		ready := cmdutil.FormatReadyReplicas(l.Status.ReadyReplicas, l.Spec.Replicas)
		phase := cmdutil.PhaseColor(l.Status.Phase)
		url := formatURL(l)

		row := []string{pterm.Cyan(l.Name), ready, phase, image, url, age}
		if showNamespace {
			row = append([]string{l.Namespace}, row...)
		}
		rows = append(rows, row)
	}

	if len(rows) == 0 {
		pterm.Info.Println("No Cluster resources found.")
		pterm.Println(pterm.Gray("Create one with: kubectl ledger create"))

		return nil
	}

	pterm.Println()
	cmdutil.RenderTable(header, rows)

	return nil
}

func formatURL(l *ledgerv1alpha1.Cluster) string {
	if l.Status.Endpoints == nil {
		return ""
	}
	if l.Status.Endpoints.GRPC != "" {
		return l.Status.Endpoints.GRPC
	}

	return l.Status.Endpoints.HTTP
}
