package restart

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

// NewCommand returns the "restart" command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	return &cobra.Command{
		Use:   "restart <name>",
		Short: "Rolling restart of a Ledger deployment",
		Long:  "Triggers a rolling restart by patching podAnnotations on the Ledger CR. The operator detects the spec hash change and performs a StatefulSet rolling update.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRestart(cmd, opts, args[0])
		},
	}
}

func runRestart(cmd *cobra.Command, opts *cmdutil.Options, name string) error {
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

	patch := client.MergeFrom(ledger.DeepCopy())
	if ledger.Spec.PodAnnotations == nil {
		ledger.Spec.PodAnnotations = make(map[string]string)
	}
	ledger.Spec.PodAnnotations["ledger.formance.com/restartedAt"] = time.Now().Format(time.RFC3339)

	if err := crdClient.Patch(ctx, ledger, patch); err != nil {
		return fmt.Errorf("restarting ledger %q: %w", name, err)
	}

	fmt.Printf("Rolling restart triggered for Ledger %q\n", name)
	return nil
}
