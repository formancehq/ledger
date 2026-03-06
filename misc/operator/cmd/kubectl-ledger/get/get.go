package get

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

// NewCommand returns the "get" command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	return &cobra.Command{
		Use:     "get [name]",
		Aliases: []string{"describe", "show", "inspect", "status", "st"},
		Short:   "Show details of a LedgerService deployment",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGet(cmd, opts, args)
		},
	}
}

func runGet(cmd *cobra.Command, opts *cmdutil.Options, args []string) error {
	ctx := cmd.Context()

	name, ns, err := cmdutil.ResolveLedgerServiceName(ctx, opts, args)
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

	spinner, _ := pterm.DefaultSpinner.Start("Fetching LedgerService details...")

	ledger, err := cmdutil.GetLedgerService(ctx, crdClient, ns, name)
	if err != nil {
		spinner.Fail("Failed to get LedgerService")

		return fmt.Errorf("getting ledger %q: %w", name, err)
	}

	switch opts.OutputFormat() {
	case "json":
		_ = spinner.Stop()

		return cmdutil.OutputJSON(ledger)
	case "yaml":
		_ = spinner.Stop()

		return cmdutil.OutputYAML(ledger)
	}

	// Overview section
	replicas := int32(3)
	if ledger.Spec.Replicas != nil {
		replicas = *ledger.Spec.Replicas
	}

	_ = spinner.Stop()
	pterm.Println()

	pterm.DefaultSection.Println("Overview")
	cmdutil.RenderTable(
		[]string{"FIELD", "VALUE"},
		[][]string{
			{"Name", pterm.Cyan(ledger.Name)},
			{"Namespace", ledger.Namespace},
			{"Phase", cmdutil.PhaseColor(ledger.Status.Phase)},
			{"Replicas", fmt.Sprintf("%d/%d", ledger.Status.ReadyReplicas, replicas)},
			{"Image", cmdutil.FormatImage(ledger.Spec.Image)},
			{"Cluster ID", ledger.Spec.Config.ClusterID},
			{"Age", cmdutil.FormatAge(time.Since(ledger.CreationTimestamp.Time))},
		},
	)

	// Pods section
	pods, err := cmdutil.LedgerServicePods(ctx, cs, ns, name)
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
				strconv.Itoa(int(cmdutil.PodRestarts(p))),
				cmdutil.FormatAge(time.Since(p.CreationTimestamp.Time)),
			})
		}
		cmdutil.RenderTable([]string{"NAME", "STATUS", "READY", "NODE", "RESTARTS", "AGE"}, podRows)
	}

	// PVCs section
	pvcs, err := cmdutil.LedgerServicePVCs(ctx, cs, ns, name)
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
			var portsSb148 strings.Builder
			for j, p := range svc.Spec.Ports {
				if j > 0 {
					portsSb148.WriteString(", ")
				}
				portsSb148.WriteString(fmt.Sprintf("%s:%d", p.Name, p.Port))
			}
			ports += portsSb148.String()
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

	// Config section
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
			{"HTTP Port", strconv.Itoa(int(cfg.HttpPort))},
			{"gRPC Port", strconv.Itoa(int(cfg.GrpcPort))},
			{"Bind Addr", cfg.BindAddr},
			{"Image", cmdutil.FormatImage(ledger.Spec.Image)},
			{"Debug", debug},
		},
	)

	return nil
}
