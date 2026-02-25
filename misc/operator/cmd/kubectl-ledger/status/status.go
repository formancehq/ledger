package status

import (
	"fmt"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"

	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

// NewCommand returns the "status" command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	return &cobra.Command{
		Use:   "status <name>",
		Short: "Show operational status of a Ledger deployment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runStatus(cmd, opts, args[0])
		},
	}
}

func runStatus(cmd *cobra.Command, opts *cmdutil.Options, name string) error {
	ctx := cmd.Context()

	ns, err := opts.ResolvedNamespace()
	if err != nil {
		return fmt.Errorf("resolving namespace: %w", err)
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating CRD client: %w", err)
	}

	cs, err := opts.Clientset()
	if err != nil {
		return fmt.Errorf("creating clientset: %w", err)
	}

	ledger, err := cmdutil.GetLedger(ctx, crdClient, ns, name)
	if err != nil {
		return fmt.Errorf("getting ledger %q: %w", name, err)
	}

	switch opts.OutputFormat() {
	case "json":
		return cmdutil.OutputJSON(ledger.Status)
	case "yaml":
		return cmdutil.OutputYAML(ledger.Status)
	}

	// Header
	replicas := int32(3)
	if ledger.Spec.Replicas != nil {
		replicas = *ledger.Spec.Replicas
	}
	fmt.Printf("Ledger %s  %s  %d/%d ready\n\n",
		pterm.Bold.Sprint(name),
		cmdutil.PhaseColor(ledger.Status.Phase),
		ledger.Status.ReadyReplicas, replicas,
	)

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
				podReadyCount(p),
				p.Spec.NodeName,
				fmt.Sprintf("%d", podRestarts(p)),
				cmdutil.FormatAge(time.Since(p.CreationTimestamp.Time)),
			})
		}
		cmdutil.RenderTable([]string{"NAME", "STATUS", "READY", "NODE", "RESTARTS", "AGE"}, podRows)
		fmt.Println()
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
		fmt.Println()
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
		fmt.Println()
	}

	// Config summary
	pterm.DefaultSection.Println("Config")
	cfg := &ledger.Spec.Config
	debug := "false"
	if cfg.Debug {
		debug = "true"
	}
	cmdutil.RenderTable(
		[]string{"KEY", "VALUE"},
		[][]string{
			{"Cluster ID", cfg.ClusterID},
			{"HTTP Port", fmt.Sprintf("%d", cfg.HttpPort)},
			{"gRPC Port", fmt.Sprintf("%d", cfg.GrpcPort)},
			{"Bind Addr", cfg.BindAddr},
			{"Image", fmt.Sprintf("%s:%s", ledger.Spec.Image.Repository, ledger.Spec.Image.Tag)},
			{"Debug", debug},
		},
	)

	return nil
}

func podReadyCount(p *corev1.Pod) string {
	ready := 0
	total := len(p.Spec.Containers)
	for _, cs := range p.Status.ContainerStatuses {
		if cs.Ready {
			ready++
		}
	}
	return fmt.Sprintf("%d/%d", ready, total)
}

func podRestarts(p *corev1.Pod) int32 {
	var restarts int32
	for _, cs := range p.Status.ContainerStatuses {
		restarts += cs.RestartCount
	}
	return restarts
}
