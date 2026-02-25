package delete

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

type deleteFlags struct {
	yes bool
}

// NewCommand returns the "delete" command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	var f deleteFlags

	cmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a Ledger deployment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDelete(cmd, opts, &f, args[0])
		},
	}

	cmd.Flags().BoolVarP(&f.yes, "yes", "y", false, "Skip confirmation")

	return cmd
}

func runDelete(cmd *cobra.Command, opts *cmdutil.Options, f *deleteFlags, name string) error {
	ctx := cmd.Context()

	ns, err := opts.ResolvedNamespace()
	if err != nil {
		return fmt.Errorf("resolving namespace: %w", err)
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating CRD client: %w", err)
	}

	ledger, err := cmdutil.GetLedger(ctx, crdClient, ns, name)
	if err != nil {
		return fmt.Errorf("getting ledger %q: %w", name, err)
	}

	if !f.yes {
		replicas := int32(3)
		if ledger.Spec.Replicas != nil {
			replicas = *ledger.Spec.Replicas
		}

		// Count PVCs
		cs, err := opts.Clientset()
		if err != nil {
			return fmt.Errorf("creating clientset: %w", err)
		}
		pvcs, err := cmdutil.LedgerPVCs(ctx, cs, ns, name)
		if err != nil {
			return fmt.Errorf("listing PVCs: %w", err)
		}

		fmt.Printf("Ledger %q in namespace %q:\n", name, ns)
		fmt.Printf("  Replicas: %d\n", replicas)
		fmt.Printf("  PVCs:     %d\n", len(pvcs.Items))
		fmt.Printf("\nDelete this Ledger? [y/N] ")

		reader := bufio.NewReader(os.Stdin)
		response, _ := reader.ReadString('\n')
		response = strings.TrimSpace(response)
		if response != "y" && response != "Y" {
			fmt.Println("Aborted.")
			return nil
		}
	}

	if err := crdClient.Delete(ctx, ledger); err != nil {
		return fmt.Errorf("deleting ledger %q: %w", name, err)
	}

	fmt.Printf("Ledger %q deleted\n", name)
	return nil
}
