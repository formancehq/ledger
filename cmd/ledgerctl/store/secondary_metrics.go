package store

import (
	"errors"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewSecondaryMetricsCommand creates the store secondary metrics command.
func NewSecondaryMetricsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "metrics",
		Short: "Get secondary store metrics",
		Long:  "Retrieve and display metrics from the secondary (read index) Pebble store via gRPC",
		RunE:  runSecondaryMetrics,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	cmd.Flags().Uint32("node-id", 0, "Target node ID (0 = local node)")

	return cmd
}

func runSecondaryMetrics(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	nodeID, _ := cmd.Flags().GetUint32("node-id")

	spinner, _ := pterm.DefaultSpinner.Start("Fetching read index metrics...")

	resp, err := client.GetSecondaryMetrics(ctx, &servicepb.GetSecondaryMetricsRequest{
		NodeId: nodeID,
	})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to get secondary metrics", err)
	}

	if !resp.GetAvailable() {
		spinner.Warning("Secondary metrics not available")

		return errors.New("secondary metrics not available")
	}

	_ = spinner.Stop()

	if handled, err := cmdutil.EncodeStructured(cmd, resp.GetMetrics()); handled || err != nil {
		return err
	}

	pterm.Println()
	printFormattedMetrics(resp.GetMetrics())

	return nil
}
