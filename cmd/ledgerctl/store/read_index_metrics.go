package store

import (
	"errors"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewReadIndexMetricsCommand creates the store read-index-metrics command.
func NewReadIndexMetricsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "read-index-metrics",
		Short: "Get read index Pebble metrics",
		Long:  "Retrieve and display metrics from the read index Pebble store via gRPC",
		RunE:  runReadIndexMetrics,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runReadIndexMetrics(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Fetching read index metrics...")

	resp, err := client.GetReadIndexMetrics(ctx, &servicepb.GetReadIndexMetricsRequest{})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to get read index metrics", err)
	}

	if !resp.GetAvailable() {
		spinner.Warning("Read index metrics not available")

		return errors.New("read index metrics not available")
	}

	_ = spinner.Stop()

	if handled, err := cmdutil.EncodeStructured(cmd, resp.GetMetrics()); handled || err != nil {
		return err
	}

	pterm.Println()
	printFormattedMetrics(resp.GetMetrics())

	return nil
}
