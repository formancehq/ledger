package cluster

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// NewBeginDrainCommand is the operator's in-Pod transport for BeginDrain.
// The command is hidden because no user-initiated shutdown workflow exists.
func NewBeginDrainCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "begin-drain <epoch>",
		Short:  "Begin an operator-controlled shutdown barrier",
		Hidden: true,
		Args:   cobra.ExactArgs(1),
		RunE:   runBeginDrain,
	}
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runBeginDrain(cmd *cobra.Command, args []string) error {
	client, conn, err := cmdutil.GetClusterClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()
	response, err := client.BeginDrain(ctx, &clusterpb.BeginDrainRequest{Epoch: args[0]})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to begin drain", err)
	}
	if handled, err := cmdutil.EncodeStructured(cmd, response); handled || err != nil {
		return err
	}
	_, err = fmt.Fprintf(cmd.OutOrStdout(), "Drain barrier at Raft index %d\n", response.GetCutoffIndex())

	return err
}
