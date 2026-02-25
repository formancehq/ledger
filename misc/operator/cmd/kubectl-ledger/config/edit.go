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
		Use:   "edit <name>",
		Short: "Edit Ledger configuration (delegates to kubectl edit)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runEdit(opts, args[0])
		},
	}
}

func runEdit(opts *cmdutil.Options, name string) error {
	ns, err := opts.ResolvedNamespace()
	if err != nil {
		return fmt.Errorf("resolving namespace: %w", err)
	}

	args := []string{"edit", "ledger.ledger.formance.com/" + name, "-n", ns}

	kubectlCmd := exec.Command("kubectl", args...)
	kubectlCmd.Stdin = os.Stdin
	kubectlCmd.Stdout = os.Stdout
	kubectlCmd.Stderr = os.Stderr

	if err := kubectlCmd.Run(); err != nil {
		return fmt.Errorf("kubectl edit failed: %w", err)
	}
	return nil
}
