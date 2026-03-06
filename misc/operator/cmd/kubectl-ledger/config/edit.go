package config

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/explain"
)

type editFlags struct {
	raw bool
}

func newEditCommand(opts *cmdutil.Options) *cobra.Command {
	flags := &editFlags{}

	cmd := &cobra.Command{
		Use:   "edit [name]",
		Short: "Edit LedgerService configuration interactively",
		Long:  "Opens an interactive editor to navigate and modify LedgerService CRD fields.\nUse --raw to delegate to kubectl edit for full YAML editing.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runEdit(cmd, opts, flags, args)
		},
	}

	cmd.Flags().BoolVar(&flags.raw, "raw", false, "Delegate to kubectl edit (raw YAML)")

	return cmd
}

func runEdit(cmd *cobra.Command, opts *cmdutil.Options, flags *editFlags, args []string) error {
	ctx := cmd.Context()

	name, ns, err := cmdutil.ResolveLedgerServiceName(ctx, opts, args)
	if err != nil {
		return err
	}

	if flags.raw {
		return runRawEdit(name, ns)
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	ledger, err := cmdutil.GetLedgerService(ctx, crdClient, ns, name)
	if err != nil {
		return fmt.Errorf("getting ledger %q: %w", name, err)
	}

	// Marshal spec to unstructured map for editing.
	specJSON, err := json.Marshal(ledger.Spec)
	if err != nil {
		return fmt.Errorf("marshaling spec: %w", err)
	}

	var working map[string]any
	if err := json.Unmarshal(specJSON, &working); err != nil {
		return fmt.Errorf("unmarshaling spec: %w", err)
	}

	original := cmdutil.DeepCopyMap(working)

	// Header.
	pterm.Println()
	pterm.Printf("Editing LedgerService %s (namespace: %s)\n",
		pterm.Bold.Sprint(pterm.Cyan(name)), pterm.Gray(ns))
	cmdutil.Separator()

	// Interactive edit loop.
	if err := cmdutil.EditLoop(explain.SpecFields(), working, "spec", true); err != nil {
		return err
	}

	// Compute diff.
	changes := cmdutil.ComputeDiff(original, working, "spec")
	if len(changes) == 0 {
		pterm.Info.Println("No changes made.")

		return nil
	}

	// Display pending changes.
	pterm.Println()
	pterm.DefaultSection.Println("Pending changes")
	rows := make([][]string, 0, len(changes))
	for _, c := range changes {
		rows = append(rows, []string{
			pterm.Cyan(c.Path),
			pterm.Gray(cmdutil.FormatChangeValue(c.Old)) + "  " + pterm.Yellow("→") + "  " + pterm.Green(cmdutil.FormatChangeValue(c.New)),
		})
	}
	cmdutil.RenderTable([]string{"FIELD", "CHANGE"}, rows)

	// Confirm.
	ok, err := cmdutil.PromptConfirm(fmt.Sprintf("Apply changes to LedgerService %s?", name), true)
	if err != nil {
		return err
	}
	if !ok {
		pterm.Info.Println("Aborted.")

		return nil
	}

	// Apply: unmarshal modified map back to typed spec.
	modJSON, err := json.Marshal(working)
	if err != nil {
		return fmt.Errorf("marshaling modified spec: %w", err)
	}

	var newSpec ledgerv1alpha1.LedgerServiceSpec
	if err := json.Unmarshal(modJSON, &newSpec); err != nil {
		return fmt.Errorf("unmarshaling modified spec: %w", err)
	}

	patch := client.MergeFrom(ledger.DeepCopy())
	ledger.Spec = newSpec
	if err := crdClient.Patch(ctx, ledger, patch); err != nil {
		return fmt.Errorf("patching ledger %q: %w", name, err)
	}

	pterm.Success.Printfln("LedgerService %s updated", name)

	return nil
}

func runRawEdit(name, ns string) error {
	kubectlArgs := []string{"edit", "ledgerservice.ledger.formance.com/" + name, "-n", ns}

	kubectlCmd := exec.Command("kubectl", kubectlArgs...)
	kubectlCmd.Stdin = os.Stdin
	kubectlCmd.Stdout = os.Stdout
	kubectlCmd.Stderr = os.Stderr

	if err := kubectlCmd.Run(); err != nil {
		return fmt.Errorf("kubectl edit failed: %w", err)
	}

	return nil
}
