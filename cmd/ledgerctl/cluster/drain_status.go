package cluster

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// NewDrainStatusCommand reads the local node's drain cursor for the operator.
func NewDrainStatusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "drain-status",
		Short:  "Read the local controlled-shutdown cursor",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE:   runDrainStatus,
	}
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runDrainStatus(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClusterClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()
	response, err := client.GetDrainStatus(ctx, &clusterpb.GetDrainStatusRequest{})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to read drain status", err)
	}
	if handled, err := cmdutil.EncodeStructured(cmd, response); handled || err != nil {
		return err
	}
	_, err = fmt.Fprintf(cmd.OutOrStdout(), "Node %d drain index %d, persisted index %d\n", response.GetLocalNode(), response.GetDrainIndex(), response.GetLastPersistedIndex())

	return err
}
