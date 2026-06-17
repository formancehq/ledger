package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// validateInterval rejects non-positive polling intervals: time.NewTicker
// panics on a non-positive duration, so the guard must run before the ticker is
// created, returning a clean error instead of letting the CLI crash.
func validateInterval(interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("--interval must be greater than 0, got %s", interval)
	}

	return nil
}

// NewWatchCommand creates the cluster watch command.
func NewWatchCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "watch",
		Short:             "Watch cluster status",
		Long:              "Continuously poll and display the cluster status (like watch)",
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runWatch,
	}

	cmd.Flags().Duration("interval", 2*time.Second, "Polling interval")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Per-request timeout")
	cmd.Flags().Uint32("node-id", 0, "Query specific node by ID (0 = route to leader)")

	return cmd
}

func runWatch(cmd *cobra.Command, _ []string) error {
	interval, _ := cmd.Flags().GetDuration("interval")
	reqTimeout, _ := cmd.Flags().GetDuration("timeout")
	nodeID, _ := cmd.Flags().GetUint32("node-id")

	if err := validateInterval(interval); err != nil {
		return err
	}

	client, conn, err := cmdutil.GetClusterClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	area, _ := pterm.DefaultArea.WithRemoveWhenDone(true).Start()

	defer func() { _ = area.Stop() }()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Poll immediately on start, then on each tick.
	for {
		output := pollClusterStatus(ctx, client, nodeID, reqTimeout, interval)
		area.Update(output)

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// pollClusterStatus performs a single gRPC call and returns the rendered output.
// On error, it returns a formatted error string instead of the status display.
func pollClusterStatus(
	ctx context.Context,
	client clusterpb.ClusterServiceClient,
	nodeID uint32,
	reqTimeout time.Duration,
	interval time.Duration,
) string {
	reqCtx, cancel := context.WithTimeout(ctx, reqTimeout)
	defer cancel()

	state, err := client.GetClusterState(reqCtx, &clusterpb.GetClusterStateRequest{
		NodeId: nodeID,
	})

	now := time.Now().Format("15:04:05")

	if err != nil {
		return pterm.Red(fmt.Sprintf("[%s] Error: %v", now, err))
	}

	return renderClusterStatus(state, false) +
		pterm.Gray(fmt.Sprintf("Last refresh: %s  (every %s)", now, interval))
}
