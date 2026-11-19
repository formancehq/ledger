package kv

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewDeleteCommand creates the KV delete command.
func NewDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "delete <key>",
		Short:             "Delete a KV key",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runDelete,
	}
	cmdutil.AddLedgerFlag(cmd)
	cmd.Flags().String("idempotency-key", "", "Idempotency key for retrying this write")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runDelete(cmd *cobra.Command, args []string) error {
	ledger, _ := cmd.Flags().GetString(cmdutil.LedgerFlagName)
	request := &servicepb.Request{Type: &servicepb.Request_DeleteKv{DeleteKv: &servicepb.DeleteKVRequest{
		Ledger: ledger, Key: args[0],
	}}}
	applyRequest, err := cmdutil.BuildApplyRequest(cmd, request)
	if err != nil {
		return err
	}
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()
	resp, err := client.Apply(ctx, applyRequest)
	if err != nil {
		return cmdutil.FormatGRPCError("failed to delete KV key", err)
	}
	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		return fmt.Errorf("response signature verification failed: %w", err)
	}

	return nil
}
