package cluster

import (
	"fmt"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewRemoveNodeCommand creates the cluster remove-node command.
func NewRemoveNodeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove-node <node-id>",
		Short: "Remove a node from the cluster",
		Long:  "Remove a node (voter or learner) from the Raft cluster. The request is forwarded to the leader. Cannot remove the leader itself; transfer leadership first.",
		Args:  cobra.ExactArgs(1),
		RunE:  runRemoveNode,
	}

	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runRemoveNode(cmd *cobra.Command, args []string) error {
	nodeID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid node ID %q: %w", args[0], err)
	}

	if nodeID == 0 {
		return fmt.Errorf("node ID must be non-zero")
	}

	client, conn, err := cmdutil.GetClusterClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	_, err = client.RemoveNode(ctx, &clusterpb.RemoveNodeRequest{
		NodeId: nodeID,
	})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to remove node", err)
	}

	pterm.Success.Printfln("Node %s removed from cluster",
		pterm.Green(fmt.Sprintf("%d", nodeID)),
	)

	return nil
}
