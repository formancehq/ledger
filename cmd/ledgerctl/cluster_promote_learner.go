package main

import (
	"fmt"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newClusterPromoteLearnerCommand creates the cluster promote-learner command.
func newClusterPromoteLearnerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "promote-learner <node-id>",
		Short: "Promote a learner node to voter",
		Long:  "Promote a learner (non-voting) node to a full voter in the Raft cluster. The request is forwarded to the leader.",
		Args:  cobra.ExactArgs(1),
		RunE:  runClusterPromoteLearner,
	}

	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runClusterPromoteLearner(cmd *cobra.Command, args []string) error {
	nodeID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid node ID %q: %w", args[0], err)
	}

	if nodeID == 0 {
		return fmt.Errorf("node ID must be non-zero")
	}

	client, conn, err := getClusterClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := getContext(cmd)
	defer cancel()

	_, err = client.PromoteLearner(ctx, &clusterpb.PromoteLearnerRequest{
		NodeId: nodeID,
	})
	if err != nil {
		return formatGRPCError("failed to promote learner", err)
	}

	pterm.Success.Printfln("Learner node %s promoted to voter",
		pterm.Green(fmt.Sprintf("%d", nodeID)),
	)

	return nil
}
