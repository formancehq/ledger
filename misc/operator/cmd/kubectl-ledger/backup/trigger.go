package backup

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
	"github.com/formancehq/ledger/misc/operator/cmd/kubectl-ledger/cmdutil"
)

type triggerFlags struct {
	wait    bool
	timeout time.Duration
}

func newTriggerCommand(opts *cmdutil.Options) *cobra.Command {
	var f triggerFlags

	cmd := &cobra.Command{
		Use:   "trigger [backup-name]",
		Short: "Manually trigger a backup by creating a BackupRun",
		Long: "Creates a BackupRun that references the given Backup. " +
			"The BackupRunReconciler executes the backup via ledgerctl. " +
			"Use --wait to block until the run reaches a terminal phase.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTrigger(cmd, opts, &f, args)
		},
	}

	cmd.Flags().BoolVar(&f.wait, "wait", false, "Wait for the run to reach a terminal phase")
	cmd.Flags().DurationVar(&f.timeout, "timeout", 30*time.Minute, "Maximum time to wait when --wait is set")

	return cmd
}

func runTrigger(cmd *cobra.Command, opts *cmdutil.Options, f *triggerFlags, args []string) error {
	ctx := cmd.Context()

	name, ns, err := cmdutil.ResolveBackupName(ctx, opts, args)
	if err != nil {
		return err
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	// Verify the parent Backup exists.
	if _, err := cmdutil.GetBackup(ctx, crdClient, ns, name); err != nil {
		return fmt.Errorf("getting Backup %q: %w", name, err)
	}

	run := &ledgerv1alpha1.BackupRun{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "ledger.formance.com/v1alpha1",
			Kind:       "BackupRun",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    ns,
			GenerateName: name + "-manual-",
			Labels:       map[string]string{ledgerv1alpha1.LabelBackup: name},
		},
		Spec: ledgerv1alpha1.BackupRunSpec{
			BackupRef: name,
		},
	}

	pterm.Info.Printfln("Creating backup run for %s...", name)
	if err := crdClient.Create(ctx, run); err != nil {
		pterm.Error.Println("Failed to create BackupRun")

		return fmt.Errorf("creating BackupRun: %w", err)
	}
	pterm.Success.Println("Created BackupRun " + pterm.Cyan(run.Name))

	if !f.wait {
		return nil
	}

	return waitForTerminal(ctx, crdClient, run, f.timeout)
}

// waitForTerminal polls the BackupRun status until it reaches Succeeded or Failed.
func waitForTerminal(ctx context.Context, c client.Client, run *ledgerv1alpha1.BackupRun, timeout time.Duration) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	pterm.Info.Println("Waiting for backup run to complete...")

	var final ledgerv1alpha1.BackupRun
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
		pterm.Error.Println("Wait failed or timed out")

		return fmt.Errorf("waiting for run: %w", err)
	}

	if final.Status.Phase == ledgerv1alpha1.BackupRunPhaseSucceeded {
		pterm.Success.Printfln("Run %s succeeded", pterm.Cyan(final.Name))
		printRunSummary(&final)

		return nil
	}

	pterm.Error.Printfln("Run %s failed", pterm.Cyan(final.Name))
	pterm.Error.Println(final.Status.Message)

	return fmt.Errorf("backup run %s failed", final.Name)
}

func printRunSummary(run *ledgerv1alpha1.BackupRun) {
	rows := [][]string{
		{"Phase", string(run.Status.Phase)},
	}
	if run.Status.StartTime != nil && run.Status.CompletionTime != nil {
		rows = append(rows, []string{"Duration", run.Status.CompletionTime.Sub(run.Status.StartTime.Time).String()})
	}
	if run.Status.Result != nil {
		rows = append(rows,
			[]string{"Files Uploaded", strconv.FormatUint(uint64(run.Status.Result.FilesUploaded), 10)},
			[]string{"Total Files", strconv.FormatUint(uint64(run.Status.Result.TotalFiles), 10)},
			[]string{"Last Log Sequence", strconv.FormatUint(run.Status.Result.LastLogSequence, 10)},
			[]string{"Last Applied Index", strconv.FormatUint(run.Status.Result.LastAppliedIndex, 10)},
		)
	}

	cmdutil.RenderBoxedTable(rows)
}
