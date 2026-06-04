package backup

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/cmdutil"
)

type triggerFlags struct {
	runType string
	wait    bool
	timeout time.Duration
}

func newTriggerCommand(opts *cmdutil.Options) *cobra.Command {
	var f triggerFlags

	cmd := &cobra.Command{
		Use:   "trigger [backup-name]",
		Short: "Manually trigger a backup by creating a LedgerBackupRun",
		Long: "Creates a LedgerBackupRun that references the given LedgerBackup. " +
			"The LedgerBackupRunReconciler executes the backup via ledgerctl. " +
			"Use --wait to block until the run reaches a terminal phase.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTrigger(cmd, opts, &f, args)
		},
	}

	cmd.Flags().StringVar(&f.runType, "type", "full", "Backup type: full or incremental")
	cmd.Flags().BoolVar(&f.wait, "wait", false, "Wait for the run to reach a terminal phase")
	cmd.Flags().DurationVar(&f.timeout, "timeout", 30*time.Minute, "Maximum time to wait when --wait is set")

	return cmd
}

func runTrigger(cmd *cobra.Command, opts *cmdutil.Options, f *triggerFlags, args []string) error {
	ctx := cmd.Context()

	runType, err := parseRunType(f.runType)
	if err != nil {
		return err
	}

	name, ns, err := cmdutil.ResolveLedgerBackupName(ctx, opts, args)
	if err != nil {
		return err
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	// Verify the parent LedgerBackup exists.
	if _, err := cmdutil.GetLedgerBackup(ctx, crdClient, ns, name); err != nil {
		return fmt.Errorf("getting LedgerBackup %q: %w", name, err)
	}

	run := &ledgerv1alpha1.LedgerBackupRun{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "ledger.formance.com/v1alpha1",
			Kind:       "LedgerBackupRun",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    ns,
			GenerateName: name + "-manual-",
			Labels: map[string]string{
				ledgerv1alpha1.LabelLedgerBackup:        name,
				ledgerv1alpha1.LabelLedgerBackupRunType: string(runType),
			},
		},
		Spec: ledgerv1alpha1.LedgerBackupRunSpec{
			BackupRef: name,
			Type:      runType,
		},
	}

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Creating %s backup run for %s...", runType, name))
	if err := crdClient.Create(ctx, run); err != nil {
		spinner.Fail("Failed to create LedgerBackupRun")

		return fmt.Errorf("creating LedgerBackupRun: %w", err)
	}
	spinner.Success("Created LedgerBackupRun " + pterm.Cyan(run.Name))

	if !f.wait {
		return nil
	}

	return waitForTerminal(ctx, crdClient, run, f.timeout)
}

func parseRunType(s string) (ledgerv1alpha1.BackupRunType, error) {
	switch strings.ToLower(s) {
	case "full", "":
		return ledgerv1alpha1.BackupRunTypeFull, nil
	case "incremental", "incr":
		return ledgerv1alpha1.BackupRunTypeIncremental, nil
	default:
		return "", fmt.Errorf("invalid --type %q (expected full or incremental)", s)
	}
}

// waitForTerminal polls the LedgerBackupRun status until it reaches Succeeded or Failed.
func waitForTerminal(ctx context.Context, c client.Client, run *ledgerv1alpha1.LedgerBackupRun, timeout time.Duration) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Waiting for backup run to complete...")

	var final ledgerv1alpha1.LedgerBackupRun
	err := wait.PollUntilContextCancel(deadlineCtx, 2*time.Second, true, func(pollCtx context.Context) (bool, error) {
		if err := c.Get(pollCtx, types.NamespacedName{
			Namespace: run.Namespace,
			Name:      run.Name,
		}, &final); err != nil {
			return false, err
		}

		return final.IsTerminal(), nil
	})
	if err != nil {
		spinner.Fail("Wait failed or timed out")

		return fmt.Errorf("waiting for run: %w", err)
	}

	if final.Status.Phase == ledgerv1alpha1.BackupRunPhaseSucceeded {
		spinner.Success(fmt.Sprintf("Run %s succeeded", pterm.Cyan(final.Name)))
		printRunSummary(&final)

		return nil
	}

	spinner.Fail(fmt.Sprintf("Run %s failed", pterm.Cyan(final.Name)))
	pterm.Error.Println(final.Status.Message)

	return fmt.Errorf("backup run %s failed", final.Name)
}

func printRunSummary(run *ledgerv1alpha1.LedgerBackupRun) {
	rows := [][]string{
		{"Phase", string(run.Status.Phase)},
		{"Type", string(run.Spec.Type)},
	}
	if run.Status.StartTime != nil && run.Status.CompletionTime != nil {
		rows = append(rows, []string{"Duration", run.Status.CompletionTime.Sub(run.Status.StartTime.Time).String()})
	}
	if run.Status.Full != nil {
		rows = append(rows,
			[]string{"Files Uploaded", strconv.FormatUint(uint64(run.Status.Full.FilesUploaded), 10)},
			[]string{"Total Files", strconv.FormatUint(uint64(run.Status.Full.TotalFiles), 10)},
			[]string{"Last Log Sequence", strconv.FormatUint(run.Status.Full.LastLogSequence, 10)},
			[]string{"Last Applied Index", strconv.FormatUint(run.Status.Full.LastAppliedIndex, 10)},
		)
	}
	if run.Status.Incremental != nil {
		rows = append(rows,
			[]string{"Log Entries Exported", strconv.FormatUint(run.Status.Incremental.LogEntriesExported, 10)},
			[]string{"Audit Entries Exported", strconv.FormatUint(run.Status.Incremental.AuditEntriesExported, 10)},
			[]string{"Segments Uploaded", strconv.FormatUint(uint64(run.Status.Incremental.SegmentsUploaded), 10)},
		)
	}

	cmdutil.RenderBoxedTable(rows)
}
