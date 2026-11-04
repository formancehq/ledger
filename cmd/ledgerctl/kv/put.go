package kv

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewPutCommand creates the KV put command.
func NewPutCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "put <key>",
		Short:             "Store raw bytes under a KV key",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runPut,
	}
	cmdutil.AddLedgerFlag(cmd)
	cmd.Flags().String("file", "", "Read the value from a file instead of stdin")
	cmd.Flags().String("idempotency-key", "", "Idempotency key for retrying this write")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runPut(cmd *cobra.Command, args []string) error {
	path, _ := cmd.Flags().GetString("file")
	var input = cmd.InOrStdin()
	if path != "" {
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("opening file %q: %w", path, err)
		}
		defer func() { _ = file.Close() }()
		input = file
	}
	value, err := io.ReadAll(io.LimitReader(input, domain.MaxKVValueBytes+1))
	if err != nil {
		return fmt.Errorf("reading KV value: %w", err)
	}
	if len(value) > domain.MaxKVValueBytes {
		return fmt.Errorf("KV value exceeds %d bytes", domain.MaxKVValueBytes)
	}
	ledger, _ := cmd.Flags().GetString(cmdutil.LedgerFlagName)
	request := &servicepb.Request{Type: &servicepb.Request_PutKv{PutKv: &servicepb.PutKVRequest{
		Ledger: ledger, Key: args[0], Value: value,
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
		return cmdutil.FormatGRPCError("failed to put KV value", err)
	}
	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		return fmt.Errorf("response signature verification failed: %w", err)
	}

	return nil
}
