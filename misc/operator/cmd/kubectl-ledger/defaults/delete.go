package defaults

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

type deleteFlags struct {
	yes bool
}

func newDeleteCommand(opts *cmdutil.Options) *cobra.Command {
	var f deleteFlags

	cmd := &cobra.Command{
		Use:     "delete [name]",
		Aliases: []string{"rm", "del"},
		Short:   "Delete a LedgerDefaults resource",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDelete(cmd, opts, &f, args)
		},
	}

	cmd.Flags().BoolVarP(&f.yes, "yes", "y", false, "Skip confirmation")

	return cmd
}

func runDelete(cmd *cobra.Command, opts *cmdutil.Options, f *deleteFlags, args []string) error {
	ctx := cmd.Context()

	name, err := cmdutil.ResolveLedgerDefaultsName(ctx, opts, args)
	if err != nil {
		return err
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating CRD client: %w", err)
	}

	defaults, err := cmdutil.GetLedgerDefaults(ctx, crdClient, name)
	if err != nil {
		return fmt.Errorf("getting ledger defaults %q: %w", name, err)
	}

	if !f.yes {
		// Warn if referenced by any LedgerServices.
		ledgers, err := cmdutil.ListLedgerServices(ctx, crdClient, "")
		if err != nil {
			return fmt.Errorf("listing ledgers: %w", err)
		}

		var refCount int
		for i := range ledgers.Items {
			if ledgers.Items[i].Spec.DefaultsRef == name {
				refCount++
			}
		}

		pterm.Println()
		rows := [][]string{
			{"Name", pterm.Cyan(name)},
			{"Scope", "Cluster"},
		}
		if refCount > 0 {
			rows = append(rows, []string{"Referenced by", pterm.Yellow(fmt.Sprintf("%d LedgerService(s)", refCount))})
		}
		cmdutil.RenderBoxedTable(rows)
		pterm.Println()

		if refCount > 0 {
			pterm.Warning.Printfln("This LedgerDefaults is referenced by %d LedgerService(s). Deleting it will cause those LedgerServices to enter Degraded state.", refCount)
		}

		confirm, err := cmdutil.PromptConfirm(
			fmt.Sprintf("Delete LedgerDefaults %s?", pterm.Cyan(name)),
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

	spinner, _ := pterm.DefaultSpinner.Start("Deleting LedgerDefaults...")

	if err := crdClient.Delete(ctx, defaults); err != nil {
		spinner.Fail("Failed to delete LedgerDefaults")
		return fmt.Errorf("deleting ledger defaults %q: %w", name, err)
	}

	spinner.Success(fmt.Sprintf("LedgerDefaults %s deleted", pterm.Cyan(name)))
	return nil
}
