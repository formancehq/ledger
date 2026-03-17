package provision

import (
	"fmt"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/pkg/scenario"
)

func NewRunCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run <scenario-name>",
		Short: "Run a provisioning scenario against the cluster",
		Long: `Run a pre-built provisioning scenario that creates ledgers, account types,
numscripts, and sample transactions against the connected cluster.

Use 'provision list' to see available scenarios.

Examples:
  ledgerctl provision run gaming-wallet --server localhost:8888 --insecure
  ledgerctl provision run marketplace`,
		Args: cobra.ExactArgs(1),
		RunE: runProvision,
	}

	cmd.Flags().Duration("timeout", 120*time.Second, "Request timeout (default 120s for provisioning)")

	return cmd
}

func runProvision(cmd *cobra.Command, args []string) error {
	name := args[0]

	fn, ok := scenario.Get(name)
	if !ok {
		pterm.Error.Printfln("Unknown scenario %q. Use 'provision list' to see available scenarios.", name)

		return cmdutil.Displayed(fmt.Errorf("unknown scenario: %s", name))
	}

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Running scenario: " + name)

	runner := scenario.NewRunner(ctx, client).WithLogger(func(step string) {
		spinner.UpdateText(step)
	})

	if err := fn(runner); err != nil {
		spinner.Fail("Scenario failed: " + err.Error())

		return cmdutil.Displayed(fmt.Errorf("scenario %s failed: %w", name, err))
	}

	spinner.Success("Scenario " + name + " completed successfully")

	return nil
}
