package cluster

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
)

// NewTransferLeaderCommand creates the cluster transfer-leader command.
func NewTransferLeaderCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "transfer-leader <node-id>",
		Aliases: []string{"tl"},
		Short:   "Transfer Raft leadership to a specific node",
		Long:    "Transfer the Raft cluster leadership to the specified node. The request is forwarded to the current leader.",
		Args:    cobra.ExactArgs(1),
		RunE:    runTransferLeader,
	}

	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runTransferLeader(cmd *cobra.Command, args []string) error {
	// Parse target node ID
	nodeID, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid node ID %q: %w", args[0], err)
	}

	if nodeID == 0 {
		return errors.New("node ID must be non-zero")
	}

	// Get gRPC connection
	client, conn, err := cmdutil.GetClusterClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	// Get context
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	// Transfer leadership
	resp, err := client.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
		Transferee: uint32(nodeID),
	})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to transfer leadership", err)
	}

	pterm.Success.Printfln("Leadership transferred to node %s", pterm.Green(strconv.FormatUint(uint64(resp.GetNewLeader()), 10)))

	return nil
}
