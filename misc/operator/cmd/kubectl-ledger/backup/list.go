package backup

import (
	"fmt"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
	"github.com/formancehq/ledger/misc/operator/cmd/kubectl-ledger/cmdutil"
)

func newListCommand(opts *cmdutil.Options) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List Backup resources in the current namespace",
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

	pterm.Info.Println("Fetching Backup resources...")
	backups, err := cmdutil.ListBackups(ctx, crdClient, ns)
	if err != nil {
		pterm.Error.Println("Failed to list Backup resources")

		return fmt.Errorf("listing backups: %w", err)
	}

	switch opts.OutputFormat() {
	case "json":
		return cmdutil.OutputJSON(backups)
	case "yaml":
		return cmdutil.OutputYAML(backups)
	default:
		return renderBackupListTable(backups)
	}
}

func renderBackupListTable(backups *ledgerv1alpha1.BackupList) error {
	header := []string{"NAME", "CLUSTER", "PHASE", "LAST BACKUP", "AGE"}

	rows := make([][]string, 0, len(backups.Items))
	for i := range backups.Items {
		b := &backups.Items[i]

		lastBackup := pterm.Gray("<never>")
		if b.Status.LastBackup != nil && b.Status.LastBackup.Time != nil {
			lastBackup = cmdutil.FormatAge(time.Since(b.Status.LastBackup.Time.Time))
		}

		rows = append(rows, []string{
			pterm.Cyan(b.Name),
			b.Spec.ClusterRef,
			cmdutil.PhaseColor(string(b.Status.Phase)),
			lastBackup,
			cmdutil.FormatAge(time.Since(b.CreationTimestamp.Time)),
		})
	}

	if len(rows) == 0 {
		pterm.Info.Println("No Backup resources found.")

		return nil
	}

	pterm.Println()
	cmdutil.RenderTable(header, rows)

	return nil
}
