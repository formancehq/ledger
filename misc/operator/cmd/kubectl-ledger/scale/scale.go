package scale

import (
	"fmt"

	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

type scaleFlags struct {
	replicas int32
}

// NewCommand returns the "scale" command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	var f scaleFlags

	cmd := &cobra.Command{
		Use:   "scale <name>",
		Short: "Scale a Ledger deployment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runScale(cmd, opts, &f, args[0])
		},
	}

	cmd.Flags().Int32Var(&f.replicas, "replicas", 0, "Number of replicas (must be odd)")
	_ = cmd.MarkFlagRequired("replicas")

	return cmd
}

func runScale(cmd *cobra.Command, opts *cmdutil.Options, f *scaleFlags, name string) error {
	if f.replicas%2 == 0 {
		return fmt.Errorf("replicas must be odd for Raft consensus, got %d", f.replicas)
	}
	if f.replicas < 1 {
		return fmt.Errorf("replicas must be at least 1, got %d", f.replicas)
	}

	ctx := cmd.Context()

	ns, err := opts.ResolvedNamespace()
	if err != nil {
		return fmt.Errorf("resolving namespace: %w", err)
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	ledger, err := cmdutil.GetLedger(ctx, crdClient, ns, name)
	if err != nil {
		return fmt.Errorf("getting ledger %q: %w", name, err)
	}

	currentReplicas := int32(3)
	if ledger.Spec.Replicas != nil {
		currentReplicas = *ledger.Spec.Replicas
	}

	if f.replicas < currentReplicas {
		fmt.Printf("Warning: scaling down from %d to %d replicas\n", currentReplicas, f.replicas)
	}

	patch := client.MergeFrom(ledger.DeepCopy())
	ledger.Spec.Replicas = &f.replicas

	if err := crdClient.Patch(ctx, ledger, patch); err != nil {
		return fmt.Errorf("scaling ledger %q: %w", name, err)
	}

	fmt.Printf("Ledger %q scaled to %d replicas\n", name, f.replicas)
	return nil
}
