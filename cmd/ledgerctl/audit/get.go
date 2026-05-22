package audit

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewGetCommand creates the audit get command.
func NewGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <sequence>",
		Short: "Get a single audit entry by sequence number",
		Long:  "Retrieve and display a single audit log entry by its sequence number",
		Args:  cobra.ExactArgs(1),
		RunE:  runGet,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runGet(cmd *cobra.Command, args []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	sequence, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid sequence number %q: %w", args[0], err)
	}

	entry, err := client.GetAuditEntry(ctx, &servicepb.GetAuditEntryRequest{
		Sequence: sequence,
	})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to get audit entry", err)
	}

	if handled, err := cmdutil.EncodeStructured(cmd, entry); handled || err != nil {
		return err
	}

	printAuditEntry(entry, true)

	return nil
}
