package status

import (
	"fmt"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

// NewCommand returns the "status" command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	return &cobra.Command{
		Use:     "status [name]",
		Aliases: []string{"st"},
		Short:   "Show operational status of a Ledger deployment",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runStatus(cmd, opts, args)
		},
	}
}

func runStatus(cmd *cobra.Command, opts *cmdutil.Options, args []string) error {
	ctx := cmd.Context()

	name, ns, err := cmdutil.ResolveLedgerName(ctx, opts, args)
	if err != nil {
		return err
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating CRD client: %w", err)
	}

	cs, err := opts.Clientset()
	if err != nil {
		return fmt.Errorf("creating clientset: %w", err)
	}

	spinner, _ := pterm.DefaultSpinner.Start("Fetching status...")

	ledger, err := cmdutil.GetLedger(ctx, crdClient, ns, name)
	if err != nil {
		spinner.Fail("Failed to get Ledger")
		return fmt.Errorf("getting ledger %q: %w", name, err)
	}

	switch opts.OutputFormat() {
	case "json":
		_ = spinner.Stop()
		return cmdutil.OutputJSON(ledger.Status)
	case "yaml":
		_ = spinner.Stop()
		return cmdutil.OutputYAML(ledger.Status)
	}

	_ = spinner.Stop()

	// Header
	replicas := int32(3)
	if ledger.Spec.Replicas != nil {
		replicas = *ledger.Spec.Replicas
	}

	pterm.Println()
	pterm.Printf("Ledger %s  %s  %d/%d ready\n",
		pterm.Bold.Sprint(pterm.Cyan(name)),
		cmdutil.PhaseColor(ledger.Status.Phase),
		ledger.Status.ReadyReplicas, replicas,
	)
	cmdutil.Separator()
	pterm.Println()

	// Pod table
	pods, err := cmdutil.LedgerPods(ctx, cs, ns, name)
	if err != nil {
		return fmt.Errorf("listing pods: %w", err)
	}

	if len(pods.Items) > 0 {
		pterm.DefaultSection.Println("Pods")
		podRows := make([][]string, 0, len(pods.Items))
		for i := range pods.Items {
			p := &pods.Items[i]
			podRows = append(podRows, []string{
				p.Name,
				string(p.Status.Phase),
				cmdutil.PodReadyCount(p),
				p.Spec.NodeName,
				fmt.Sprintf("%d", cmdutil.PodRestarts(p)),
				cmdutil.FormatAge(time.Since(p.CreationTimestamp.Time)),
			})
		}
		cmdutil.RenderTable([]string{"NAME", "STATUS", "READY", "NODE", "RESTARTS", "AGE"}, podRows)
		pterm.Println()
	}

	// PVC table
	pvcs, err := cmdutil.LedgerPVCs(ctx, cs, ns, name)
	if err != nil {
		return fmt.Errorf("listing PVCs: %w", err)
	}

	if len(pvcs.Items) > 0 {
		pterm.DefaultSection.Println("PVCs")
		pvcRows := make([][]string, 0, len(pvcs.Items))
		for i := range pvcs.Items {
			pvc := &pvcs.Items[i]
			capacity := ""
			if qty, ok := pvc.Status.Capacity["storage"]; ok {
				capacity = qty.String()
			}
			sc := ""
			if pvc.Spec.StorageClassName != nil {
				sc = *pvc.Spec.StorageClassName
			}
			pvcRows = append(pvcRows, []string{
				pvc.Name,
				string(pvc.Status.Phase),
				capacity,
				sc,
			})
		}
		cmdutil.RenderTable([]string{"NAME", "STATUS", "CAPACITY", "STORAGE CLASS"}, pvcRows)
		pterm.Println()
	}

	// Conditions table
	if len(ledger.Status.Conditions) > 0 {
		pterm.DefaultSection.Println("Conditions")
		condRows := make([][]string, 0, len(ledger.Status.Conditions))
		for i := range ledger.Status.Conditions {
			c := &ledger.Status.Conditions[i]
			condRows = append(condRows, []string{
				c.Type,
				string(c.Status),
				c.Reason,
				c.Message,
				cmdutil.FormatAge(time.Since(c.LastTransitionTime.Time)),
			})
		}
		cmdutil.RenderTable([]string{"TYPE", "STATUS", "REASON", "MESSAGE", "AGE"}, condRows)
		pterm.Println()
	}

	// Config summary
	pterm.DefaultSection.Println("Config")
	cfg := &ledger.Spec.Config
	debug := "false"
	if cfg.Debug {
		debug = pterm.Yellow("true")
	}
	cmdutil.RenderTable(
		[]string{"KEY", "VALUE"},
		[][]string{
			{"Cluster ID", cfg.ClusterID},
			{"HTTP Port", fmt.Sprintf("%d", cfg.HttpPort)},
			{"gRPC Port", fmt.Sprintf("%d", cfg.GrpcPort)},
			{"Bind Addr", cfg.BindAddr},
			{"Image", cmdutil.FormatImage(ledger.Spec.Image)},
			{"Debug", debug},
		},
	)

	return nil
}
