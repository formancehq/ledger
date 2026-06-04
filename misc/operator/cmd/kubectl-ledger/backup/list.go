package backup

import (
	"fmt"
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
		Short:   "List LedgerBackup resources in the current namespace",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runList(cmd, opts)
		},
	}
}

func runList(cmd *cobra.Command, opts *cmdutil.Options) error {
	ctx := cmd.Context()

	ns, err := opts.ResolvedNamespace()
	if err != nil {
		return fmt.Errorf("resolving namespace: %w", err)
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	spinner, _ := pterm.DefaultSpinner.Start("Fetching LedgerBackup resources...")
	backups, err := cmdutil.ListLedgerBackups(ctx, crdClient, ns)
	if err != nil {
		spinner.Fail("Failed to list LedgerBackup resources")

		return fmt.Errorf("listing backups: %w", err)
	}
	_ = spinner.Stop()

	switch opts.OutputFormat() {
	case "json":
		return cmdutil.OutputJSON(backups)
	case "yaml":
		return cmdutil.OutputYAML(backups)
	default:
		return renderBackupListTable(backups)
	}
}

func renderBackupListTable(backups *ledgerv1alpha1.LedgerBackupList) error {
	header := []string{"NAME", "SERVICE", "PHASE", "LAST FULL", "LAST INCR", "AGE"}

	rows := make([][]string, 0, len(backups.Items))
	for i := range backups.Items {
		b := &backups.Items[i]

		lastFull := pterm.Gray("<never>")
		if b.Status.LastFullBackup != nil && b.Status.LastFullBackup.Time != nil {
			lastFull = cmdutil.FormatAge(time.Since(b.Status.LastFullBackup.Time.Time))
		}
		lastIncr := pterm.Gray("<never>")
		if b.Status.LastIncrementalBackup != nil && b.Status.LastIncrementalBackup.Time != nil {
			lastIncr = cmdutil.FormatAge(time.Since(b.Status.LastIncrementalBackup.Time.Time))
		}

		rows = append(rows, []string{
			pterm.Cyan(b.Name),
			b.Spec.ServiceRef,
			cmdutil.PhaseColor(string(b.Status.Phase)),
			lastFull,
			lastIncr,
			cmdutil.FormatAge(time.Since(b.CreationTimestamp.Time)),
		})
	}

	if len(rows) == 0 {
		pterm.Info.Println("No LedgerBackup resources found.")

		return nil
	}

	pterm.Println()
	cmdutil.RenderTable(header, rows)

	return nil
}
