package delete

import (
	"fmt"

	"github.com/pterm/pterm"
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
		Use:     "delete [name]",
		Aliases: []string{"rm", "del"},
		Short:   "Delete a LedgerService deployment",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDelete(cmd, opts, &f, args)
		},
	}

	cmd.Flags().BoolVarP(&f.yes, "yes", "y", false, "Skip confirmation")

	return cmd
}

func runDelete(cmd *cobra.Command, opts *cmdutil.Options, f *deleteFlags, args []string) error {
	ctx := cmd.Context()

	name, ns, err := cmdutil.ResolveLedgerServiceName(ctx, opts, args)
	if err != nil {
		return err
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating CRD client: %w", err)
	}

	ledger, err := cmdutil.GetLedgerService(ctx, crdClient, ns, name)
	if err != nil {
		return fmt.Errorf("getting ledger %q: %w", name, err)
	}

	if !f.yes {
		replicas := int32(3)
		if ledger.Spec.Replicas != nil {
			replicas = *ledger.Spec.Replicas
		}

		cs, err := opts.Clientset()
		if err != nil {
			return fmt.Errorf("creating clientset: %w", err)
		}
		pvcs, err := cmdutil.LedgerServicePVCs(ctx, cs, ns, name)
		if err != nil {
			return fmt.Errorf("listing PVCs: %w", err)
		}

		pterm.Println()
		cmdutil.RenderBoxedTable([][]string{
			{"Name", pterm.Cyan(name)},
			{"Namespace", ns},
			{"Image", cmdutil.FormatImage(ledger.Spec.Image)},
			{"Replicas", fmt.Sprintf("%d", replicas)},
			{"PVCs", fmt.Sprintf("%d", len(pvcs.Items))},
		})
		pterm.Println()

		confirm, err := cmdutil.PromptConfirm(
			fmt.Sprintf("Delete LedgerService %s?", pterm.Cyan(name)),
			false,
		)
		if err != nil {
			return err
		}
		if !confirm {
			pterm.Warning.Println("Aborted.")
			return nil
		}
	}

	spinner, _ := pterm.DefaultSpinner.Start("Deleting LedgerService...")

	if err := crdClient.Delete(ctx, ledger); err != nil {
		spinner.Fail("Failed to delete LedgerService")
		return fmt.Errorf("deleting ledger %q: %w", name, err)
	}

	spinner.Success(fmt.Sprintf("LedgerService %s deleted", pterm.Cyan(name)))
	return nil
}
