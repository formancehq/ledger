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

// NewPromoteLearnerCommand creates the cluster promote-learner command.
func NewPromoteLearnerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "promote-learner <node-id>",
		Short: "Promote a learner node to voter",
		Long:  "Promote a learner (non-voting) node to a full voter in the Raft cluster. The request is forwarded to the leader.",
		Args:  cobra.ExactArgs(1),
		RunE:  runPromoteLearner,
	}

	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runPromoteLearner(cmd *cobra.Command, args []string) error {
	nodeID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid node ID %q: %w", args[0], err)
	}

	if nodeID == 0 {
		return errors.New("node ID must be non-zero")
	}

	client, conn, err := cmdutil.GetClusterClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	_, err = client.PromoteLearner(ctx, &clusterpb.PromoteLearnerRequest{
		NodeId: nodeID,
	})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to promote learner", err)
	}

	pterm.Success.Printfln("Learner node %s promoted to voter",
		pterm.Green(strconv.FormatUint(nodeID, 10)),
	)

	return nil
}
