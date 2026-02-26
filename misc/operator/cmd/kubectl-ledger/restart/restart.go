package restart

import (
	"fmt"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

type restartFlags struct {
	yes bool
}

// NewCommand returns the "restart" command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	var f restartFlags

	cmd := &cobra.Command{
		Use:     "restart [name]",
		Aliases: []string{"rollout"},
		Short:   "Rolling restart of a LedgerService deployment",
		Long:  "Triggers a rolling restart by patching podAnnotations on the LedgerService CR. The operator detects the spec hash change and performs a StatefulSet rolling update.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRestart(cmd, opts, &f, args)
		},
	}

	cmd.Flags().BoolVarP(&f.yes, "yes", "y", false, "Skip confirmation")

	return cmd
}

func runRestart(cmd *cobra.Command, opts *cmdutil.Options, f *restartFlags, args []string) error {
	ctx := cmd.Context()

	name, ns, err := cmdutil.ResolveLedgerServiceName(ctx, opts, args)
	if err != nil {
		return err
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	ledger, err := cmdutil.GetLedgerService(ctx, crdClient, ns, name)
	if err != nil {
		return fmt.Errorf("getting ledger %q: %w", name, err)
	}

	pterm.Info.Printfln("Image: %s", cmdutil.FormatImage(ledger.Spec.Image))

	if !f.yes {
		confirm, err := cmdutil.PromptConfirm(
			fmt.Sprintf("Rolling restart LedgerService %s?", pterm.Cyan(name)),
			true,
		)
		if err != nil {
			return err
		}
		if !confirm {
			pterm.Warning.Println("Aborted.")
			return nil
		}
	}

	patch := client.MergeFrom(ledger.DeepCopy())
	if ledger.Spec.PodAnnotations == nil {
		ledger.Spec.PodAnnotations = make(map[string]string)
	}
	ledger.Spec.PodAnnotations["ledger.formance.com/restartedAt"] = time.Now().Format(time.RFC3339)

	spinner, _ := pterm.DefaultSpinner.Start("Triggering rolling restart...")

	if err := crdClient.Patch(ctx, ledger, patch); err != nil {
		spinner.Fail("Failed to restart LedgerService")
		return fmt.Errorf("restarting ledger %q: %w", name, err)
	}

	spinner.Success(fmt.Sprintf("Rolling restart triggered for LedgerService %s", pterm.Cyan(name)))
	return nil
}
