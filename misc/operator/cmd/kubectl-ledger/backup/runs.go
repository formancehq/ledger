package backup

import (
	"fmt"
	"sort"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
	"github.com/formancehq/ledger/misc/operator/cmd/kubectl-ledger/cmdutil"
)

func newRunsCommand(opts *cmdutil.Options) *cobra.Command {
	return &cobra.Command{
		Use:   "runs [backup-name]",
		Short: "List BackupRun resources for a Backup",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRuns(cmd, opts, args)
		},
	}
}

func runRuns(cmd *cobra.Command, opts *cmdutil.Options, args []string) error {
	ctx := cmd.Context()

	name, ns, err := cmdutil.ResolveBackupName(ctx, opts, args)
	if err != nil {
		return err
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	spinner, _ := pterm.DefaultSpinner.Start("Fetching BackupRun resources...")
	runs, err := cmdutil.ListBackupRuns(ctx, crdClient, ns, name)
	if err != nil {
		spinner.Fail("Failed to list BackupRun resources")

		return fmt.Errorf("listing runs: %w", err)
	}
	_ = spinner.Stop()

	sort.Slice(runs.Items, func(i, j int) bool {
		return runs.Items[i].CreationTimestamp.After(runs.Items[j].CreationTimestamp.Time)
	})

	switch opts.OutputFormat() {
	case "json":
		return cmdutil.OutputJSON(runs)
	case "yaml":
		return cmdutil.OutputYAML(runs)
	default:
		return renderRunsTable(runs)
	}
}

func renderRunsTable(runs *ledgerv1alpha1.BackupRunList) error {
	header := []string{"NAME", "TYPE", "PHASE", "STARTED", "DURATION", "AGE"}

	rows := make([][]string, 0, len(runs.Items))
	for i := range runs.Items {
		r := &runs.Items[i]

		started := pterm.Gray("<pending>")
		if r.Status.StartTime != nil {
			started = cmdutil.FormatAge(time.Since(r.Status.StartTime.Time))
		}

		duration := pterm.Gray("-")
		if r.Status.StartTime != nil && r.Status.CompletionTime != nil {
			duration = r.Status.CompletionTime.Sub(r.Status.StartTime.Time).Truncate(time.Millisecond).String()
		}

		rows = append(rows, []string{
			pterm.Cyan(r.Name),
			string(r.Spec.Type),
			runPhaseColor(string(r.Status.Phase)),
			started,
			duration,
			cmdutil.FormatAge(time.Since(r.CreationTimestamp.Time)),
		})
	}

	if len(rows) == 0 {
		pterm.Info.Println("No BackupRun resources found for this backup.")

		return nil
	}

	pterm.Println()
	cmdutil.RenderTable(header, rows)

	return nil
}

// runPhaseColor colors a BackupRun phase string for terminal output.
func runPhaseColor(phase string) string {
	switch phase {
	case string(ledgerv1alpha1.BackupRunPhaseSucceeded):
		return pterm.FgGreen.Sprint(phase)
	case string(ledgerv1alpha1.BackupRunPhaseFailed):
		return pterm.FgRed.Sprint(phase)
	case string(ledgerv1alpha1.BackupRunPhaseRunning):
		return pterm.FgYellow.Sprint(phase)
	default:
		if phase == "" {
			phase = string(ledgerv1alpha1.BackupRunPhasePending)
		}

		return pterm.FgGray.Sprint(phase)
	}
}
