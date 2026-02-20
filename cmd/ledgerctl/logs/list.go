package logs

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewListCommand creates the logs list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "l"},
		Short:   "List system logs",
		Long:    "List system log entries via gRPC streaming",
		RunE:    runList,
	}

	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Uint64("after", 0, "Show logs after this sequence number")
	cmd.Flags().Int("limit", 0, "Maximum number of logs to display (0 = unlimited)")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runList(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	var (
		jsonOutput, _ = cmd.Flags().GetBool("json")
		after, _      = cmd.Flags().GetUint64("after")
		limit, _      = cmd.Flags().GetInt("limit")
	)

	req := &servicepb.ListLogsRequest{}
	if cmd.Flags().Changed("after") {
		req.AfterSequence = &after
	}
	if limit > 0 {
		req.PageSize = uint32(limit)
	}

	stream, err := client.ListLogs(ctx, req)
	if err != nil {
		return cmdutil.FormatGRPCError("failed to list logs", err)
	}

	var entries []*commonpb.Log
	for {
		log, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return cmdutil.FormatGRPCError("receiving log", err)
		}
		entries = append(entries, log)
		if limit > 0 && len(entries) >= limit {
			break
		}
	}

	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(entries)
	}

	if len(entries) == 0 {
		pterm.Println("No logs found.")
		return nil
	}

	for _, log := range entries {
		printLog(log)
	}

	pterm.Println()
	pterm.Info.Printfln("%d log(s) displayed", len(entries))

	return nil
}

// printLog prints a single system log in a human-readable format.
func printLog(log *commonpb.Log) {
	typeDesc, ledgerName := describeLogPayload(log)

	ledgerStr := ""
	if ledgerName != "" {
		ledgerStr = fmt.Sprintf(" ledger=%s", pterm.Cyan(ledgerName))
	}

	pterm.Printf("  #%-6d %s%s  %s\n",
		log.Sequence,
		pterm.Green(typeDesc),
		ledgerStr,
		pterm.Gray(formatLogDetails(log)),
	)
}

// describeLogPayload returns a human-readable type and ledger name from a log payload.
func describeLogPayload(log *commonpb.Log) (string, string) {
	if log.Payload == nil {
		return "UNKNOWN", ""
	}
	switch t := log.Payload.Type.(type) {
	case *commonpb.LogPayload_CreateLedger:
		name := ""
		if t.CreateLedger != nil && t.CreateLedger.Info != nil {
			name = t.CreateLedger.Info.Name
		}
		return "CREATE_LEDGER", name
	case *commonpb.LogPayload_DeleteLedger:
		name := ""
		if t.DeleteLedger != nil && t.DeleteLedger.Info != nil {
			name = t.DeleteLedger.Info.Name
		}
		return "DELETE_LEDGER", name
	case *commonpb.LogPayload_Apply:
		if t.Apply == nil || t.Apply.Log == nil {
			return "APPLY", ""
		}
		ledgerName := t.Apply.LedgerName
		switch t.Apply.Log.Data.Payload.(type) {
		case *commonpb.LedgerLogPayload_CreatedTransaction:
			return "CREATE_TX", ledgerName
		case *commonpb.LedgerLogPayload_RevertedTransaction:
			return "REVERT_TX", ledgerName
		case *commonpb.LedgerLogPayload_SavedMetadata:
			return "SAVE_METADATA", ledgerName
		case *commonpb.LedgerLogPayload_DeletedMetadata:
			return "DELETE_METADATA", ledgerName
		default:
			return "APPLY", ledgerName
		}
	case *commonpb.LogPayload_RegisterSigningKey:
		return "REGISTER_KEY", ""
	case *commonpb.LogPayload_RevokeSigningKey:
		return "REVOKE_KEY", ""
	case *commonpb.LogPayload_SetSigningConfig:
		return "SET_SIGNING_CFG", ""
	case *commonpb.LogPayload_AddedEventsSink:
		return "ADD_SINK", ""
	case *commonpb.LogPayload_RemovedEventsSink:
		return "REMOVE_SINK", ""
	case *commonpb.LogPayload_ClosePeriod:
		return "CLOSE_PERIOD", ""
	case *commonpb.LogPayload_SealPeriod:
		return "SEAL_PERIOD", ""
	case *commonpb.LogPayload_ArchivePeriod:
		return "ARCHIVE_PERIOD", ""
	case *commonpb.LogPayload_ConfirmArchivePeriod:
		return "CONFIRM_ARCHIVE", ""
	case *commonpb.LogPayload_SetMaintenanceMode:
		return "MAINTENANCE", ""
	default:
		return "UNKNOWN", ""
	}
}

// formatLogDetails returns additional details for a log entry.
func formatLogDetails(log *commonpb.Log) string {
	if log.Payload == nil {
		return ""
	}
	switch t := log.Payload.Type.(type) {
	case *commonpb.LogPayload_Apply:
		if t.Apply == nil || t.Apply.Log == nil {
			return ""
		}
		ledgerLog := t.Apply.Log
		ts := ""
		if ledgerLog.Date != nil {
			ts = ledgerLog.Date.AsTime().Format(time.RFC3339)
		}
		switch p := ledgerLog.Data.Payload.(type) {
		case *commonpb.LedgerLogPayload_CreatedTransaction:
			if p.CreatedTransaction != nil && p.CreatedTransaction.Transaction != nil {
				return fmt.Sprintf("tx=%d %s", p.CreatedTransaction.Transaction.Id, ts)
			}
		case *commonpb.LedgerLogPayload_RevertedTransaction:
			if p.RevertedTransaction != nil {
				return fmt.Sprintf("reverted_tx=%d %s", p.RevertedTransaction.RevertedTransactionId, ts)
			}
		}
		return ts
	case *commonpb.LogPayload_CreateLedger:
		if t.CreateLedger != nil && t.CreateLedger.Info != nil && t.CreateLedger.Info.CreatedAt != nil {
			return t.CreateLedger.Info.CreatedAt.AsTime().Format(time.RFC3339)
		}
	}
	return ""
}
