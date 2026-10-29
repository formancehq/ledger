package cluster

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// NewEndDrainCommand is the operator's in-Pod transport for a Raft-ordered
// drain release. It is hidden with the companion begin-drain command.
func NewEndDrainCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "end-drain <epoch>",
		Short:  "Release an operator-controlled shutdown barrier",
		Hidden: true,
		Args:   cobra.ExactArgs(1),
		RunE:   runEndDrain,
	}
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runEndDrain(cmd *cobra.Command, args []string) error {
	client, conn, err := cmdutil.GetClusterClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()
	response, err := client.EndDrain(ctx, &clusterpb.EndDrainRequest{Epoch: args[0]})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to end drain", err)
	}
	if handled, err := cmdutil.EncodeStructured(cmd, response); handled || err != nil {
		return err
	}
	_, err = fmt.Fprintf(cmd.OutOrStdout(), "Drain released at Raft index %d\n", response.GetReleaseIndex())

	return err
}
