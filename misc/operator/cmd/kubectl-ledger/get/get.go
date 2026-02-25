package get

import (
	"fmt"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"

	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

// NewCommand returns the "get" command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	return &cobra.Command{
		Use:   "get <name>",
		Short: "Show details of a Ledger deployment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGet(cmd, opts, args[0])
		},
	}
}

func runGet(cmd *cobra.Command, opts *cmdutil.Options, name string) error {
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
		return cmdutil.OutputJSON(ledger)
	case "yaml":
		return cmdutil.OutputYAML(ledger)
	}

	// Overview section
	replicas := int32(3)
	if ledger.Spec.Replicas != nil {
		replicas = *ledger.Spec.Replicas
	}

	pterm.DefaultSection.Println("Overview")
	cmdutil.RenderTable(
		[]string{"FIELD", "VALUE"},
		[][]string{
			{"Name", ledger.Name},
			{"Namespace", ledger.Namespace},
			{"Phase", cmdutil.PhaseColor(ledger.Status.Phase)},
			{"Replicas", fmt.Sprintf("%d/%d", ledger.Status.ReadyReplicas, replicas)},
			{"Image", fmt.Sprintf("%s:%s", ledger.Spec.Image.Repository, ledger.Spec.Image.Tag)},
			{"Cluster ID", ledger.Spec.Config.ClusterID},
			{"Age", cmdutil.FormatAge(time.Since(ledger.CreationTimestamp.Time))},
		},
	)

	// Pods section
	pods, err := cmdutil.LedgerPods(ctx, cs, ns, name)
	if err != nil {
		return fmt.Errorf("listing pods: %w", err)
	}

	if len(pods.Items) > 0 {
		pterm.DefaultSection.Println("Pods")
		podRows := make([][]string, 0, len(pods.Items))
		for i := range pods.Items {
			p := &pods.Items[i]
			ready := podReadyCount(p)
			podRows = append(podRows, []string{
				p.Name,
				string(p.Status.Phase),
				ready,
				p.Spec.NodeName,
				fmt.Sprintf("%d", podRestarts(p)),
				cmdutil.FormatAge(time.Since(p.CreationTimestamp.Time)),
			})
		}
		cmdutil.RenderTable([]string{"NAME", "STATUS", "READY", "NODE", "RESTARTS", "AGE"}, podRows)
	}

	// PVCs section
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
	}

	// Services section
	svcs, err := cmdutil.LedgerServices(ctx, cs, ns, name)
	if err != nil {
		return fmt.Errorf("listing services: %w", err)
	}

	if len(svcs.Items) > 0 {
		pterm.DefaultSection.Println("Services")
		svcRows := make([][]string, 0, len(svcs.Items))
		for i := range svcs.Items {
			svc := &svcs.Items[i]
			ports := ""
			for j, p := range svc.Spec.Ports {
				if j > 0 {
					ports += ", "
				}
				ports += fmt.Sprintf("%s:%d", p.Name, p.Port)
			}
			svcRows = append(svcRows, []string{
				svc.Name,
				string(svc.Spec.Type),
				svc.Spec.ClusterIP,
				ports,
			})
		}
		cmdutil.RenderTable([]string{"NAME", "TYPE", "CLUSTER-IP", "PORTS"}, svcRows)
	}

	// Conditions section
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
	}

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
