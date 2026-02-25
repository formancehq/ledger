package list

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
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
		Short:   "List Ledger deployments",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runList(cmd, opts, &f)
		},
	}

	cmd.Flags().BoolVarP(&f.allNamespaces, "all-namespaces", "A", false, "List across all namespaces")

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

	ledgers, err := cmdutil.ListLedgers(ctx, crdClient, ns)
	if err != nil {
		return fmt.Errorf("listing ledgers: %w", err)
	}

	switch opts.OutputFormat() {
	case "json":
		return cmdutil.OutputJSON(ledgers)
	case "yaml":
		return cmdutil.OutputYAML(ledgers)
	default:
		return renderTable(ledgers, f.allNamespaces)
	}
}

func renderTable(ledgers *ledgerv1alpha1.LedgerList, showNamespace bool) error {
	header := []string{"NAME", "REPLICAS", "PHASE", "IMAGE", "AGE"}
	if showNamespace {
		header = append([]string{"NAMESPACE"}, header...)
	}

	rows := make([][]string, 0, len(ledgers.Items))
	for i := range ledgers.Items {
		l := &ledgers.Items[i]
		image := fmt.Sprintf("%s:%s", l.Spec.Image.Repository, l.Spec.Image.Tag)
		age := cmdutil.FormatAge(time.Since(l.CreationTimestamp.Time))
		ready := cmdutil.FormatReadyReplicas(l.Status.ReadyReplicas, l.Spec.Replicas)
		phase := cmdutil.PhaseColor(l.Status.Phase)

		row := []string{l.Name, ready, phase, image, age}
		if showNamespace {
			row = append([]string{l.Namespace}, row...)
		}
		rows = append(rows, row)
	}

	if len(rows) == 0 {
		fmt.Println("No Ledger resources found.")
		return nil
	}

	cmdutil.RenderTable(header, rows)
	return nil
}
