package main

import (
	"fmt"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newClusterAddLearnerCommand creates the cluster add-learner command.
func newClusterAddLearnerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add-learner <node-id> <raft-address> <service-address>",
		Short: "Add a non-voting learner node to the cluster",
		Long:  "Add a learner (non-voting) node to the Raft cluster. The request is forwarded to the leader.",
		Args:  cobra.ExactArgs(3),
		RunE:  runClusterAddLearner,
	}

	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runClusterAddLearner(cmd *cobra.Command, args []string) error {
	nodeID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid node ID %q: %w", args[0], err)
	}

	if nodeID == 0 {
		return fmt.Errorf("node ID must be non-zero")
	}

	raftAddress := args[1]
	serviceAddress := args[2]

	client, conn, err := getClusterClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := getContext(cmd)
	defer cancel()

	_, err = client.AddLearner(ctx, &clusterpb.AddLearnerRequest{
		NodeId:         nodeID,
		RaftAddress:    raftAddress,
		ServiceAddress: serviceAddress,
	})
	if err != nil {
		return formatGRPCError("failed to add learner", err)
	}

	pterm.Success.Printfln("Learner node %s added to cluster (raft=%s, service=%s)",
		pterm.Green(fmt.Sprintf("%d", nodeID)),
		pterm.Cyan(raftAddress),
		pterm.Cyan(serviceAddress),
	)

	return nil
}
