package cluster

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// NewAddLearnerCommand creates the cluster add-learner command.
func NewAddLearnerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add-learner <node-id> <raft-address> <service-address>",
		Short: "Add a non-voting learner node to the cluster",
		Long:  "Add a learner (non-voting) node to the Raft cluster. The request is forwarded to the leader.",
		Args:  cobra.ExactArgs(3),
		RunE:  runAddLearner,
	}

	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runAddLearner(cmd *cobra.Command, args []string) error {
	nodeID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid node ID %q: %w", args[0], err)
	}

	if nodeID == 0 {
		return errors.New("node ID must be non-zero")
	}

	raftAddress := args[1]
	serviceAddress := args[2]

	client, conn, err := cmdutil.GetClusterClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	_, err = client.AddLearner(ctx, &clusterpb.AddLearnerRequest{
		NodeId:         nodeID,
		RaftAddress:    raftAddress,
		ServiceAddress: serviceAddress,
	})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to add learner", err)
	}

	pterm.Success.Printfln("Learner node %s added to cluster (raft=%s, service=%s)",
		pterm.Green(strconv.FormatUint(nodeID, 10)),
		pterm.Cyan(raftAddress),
		pterm.Cyan(serviceAddress),
	)

	return nil
}
