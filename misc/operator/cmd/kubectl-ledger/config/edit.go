package config

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

func newEditCommand(opts *cmdutil.Options) *cobra.Command {
	return &cobra.Command{
		Use:   "edit [name]",
		Short: "Edit Ledger configuration (delegates to kubectl edit)",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runEdit(cmd, opts, args)
		},
	}
}

func runEdit(cmd *cobra.Command, opts *cmdutil.Options, args []string) error {
	name, ns, err := cmdutil.ResolveLedgerName(cmd.Context(), opts, args)
	if err != nil {
		return err
	}

	kubectlArgs := []string{"edit", "ledger.ledger.formance.com/" + name, "-n", ns}

	kubectlCmd := exec.Command("kubectl", kubectlArgs...)
	kubectlCmd.Stdin = os.Stdin
	kubectlCmd.Stdout = os.Stdout
	kubectlCmd.Stderr = os.Stderr

	if err := kubectlCmd.Run(); err != nil {
		return fmt.Errorf("kubectl edit failed: %w", err)
	}
	return nil
}
