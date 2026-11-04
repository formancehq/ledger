package kv

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewGetCommand creates the KV get command.
func NewGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "get <key>",
		Short:             "Write a KV value as raw bytes",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runGet,
	}
	cmdutil.AddLedgerFlag(cmd)
	cmdutil.AddConsistencyFlags(cmd)
	cmd.Flags().String("file", "", "Write the value to a file instead of stdout")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runGet(cmd *cobra.Command, args []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ledger, _ := cmd.Flags().GetString(cmdutil.LedgerFlagName)
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()
	resp, err := client.GetKV(ctx, &servicepb.GetKVRequest{
		Ledger: ledger,
		Key:    args[0],
		Read:   cmdutil.BuildReadOptions(cmdutil.GetConsistencyFlags(cmd)),
	})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to get KV value", err)
	}
	path, _ := cmd.Flags().GetString("file")
	if path != "" {
		if err := os.WriteFile(path, resp.GetValue(), 0600); err != nil {
			return fmt.Errorf("writing file %q: %w", path, err)
		}

		return nil
	}
	if _, err := io.Copy(cmd.OutOrStdout(), bytes.NewReader(resp.GetValue())); err != nil {
		return fmt.Errorf("writing KV value: %w", err)
	}

	return nil
}
