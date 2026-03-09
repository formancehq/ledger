package logs

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
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

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Uint64("after", 0, "Show logs after this sequence number")
	cmd.Flags().Uint32("page-size", cmdutil.DefaultPageSize, "Number of logs per page (0 = unlimited)")
	cmd.Flags().Uint64("min-log-sequence", 0, "Minimum log sequence the server must have applied before reading (0 = no constraint)")
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
		after, _     = cmd.Flags().GetUint64("after")
		pageSize, _  = cmd.Flags().GetUint32("page-size")
		minLogSeq, _ = cmd.Flags().GetUint64("min-log-sequence")
	)

	req := &servicepb.ListLogsRequest{
		PageSize:       pageSize,
		MinLogSequence: minLogSeq,
	}
	if cmd.Flags().Changed("after") {
		req.AfterSequence = &after
	}

	stream, err := client.ListLogs(ctx, req)
	if err != nil {
		return cmdutil.FormatGRPCError("failed to list logs", err)
	}

	var entries []*commonpb.Log

	for {
		log, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return cmdutil.FormatGRPCError("receiving log", err)
		}

		entries = append(entries, log)
		if pageSize > 0 && uint32(len(entries)) >= pageSize {
			break
		}
	}

	if handled, err := cmdutil.EncodeStructured(cmd, entries); handled || err != nil {
		return err
	}

	if len(entries) == 0 {
		pterm.Info.Println("No logs found.")

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
		ledgerStr = " ledger=" + pterm.Cyan(ledgerName)
	}

	pterm.Printf("  #%-6d %s%s  %s\n",
		log.GetSequence(),
		pterm.Green(typeDesc),
		ledgerStr,
		pterm.Gray(formatLogDetails(log)),
	)
}

// describeLogPayload returns a human-readable type and ledger name from a log payload.
func describeLogPayload(log *commonpb.Log) (string, string) {
	if log.GetPayload() == nil {
		return "UNKNOWN", ""
	}

	switch t := log.GetPayload().GetType().(type) {
	case *commonpb.LogPayload_CreateLedger:
		name := ""
		if t.CreateLedger != nil && t.CreateLedger.GetInfo() != nil {
			name = t.CreateLedger.GetInfo().GetName()
		}

		return "CREATE_LEDGER", name
	case *commonpb.LogPayload_DeleteLedger:
		name := ""
		if t.DeleteLedger != nil && t.DeleteLedger.GetInfo() != nil {
			name = t.DeleteLedger.GetInfo().GetName()
		}

		return "DELETE_LEDGER", name
	case *commonpb.LogPayload_Apply:
		if t.Apply == nil || t.Apply.GetLog() == nil {
			return "APPLY", ""
		}

		ledgerName := t.Apply.GetLedgerName()
		switch t.Apply.GetLog().GetData().GetPayload().(type) {
		case *commonpb.LedgerLogPayload_CreatedTransaction:
			return "CREATE_TX", ledgerName
		case *commonpb.LedgerLogPayload_RevertedTransaction:
			return "REVERT_TX", ledgerName
		case *commonpb.LedgerLogPayload_SavedMetadata:
			return "SAVE_METADATA", ledgerName
		case *commonpb.LedgerLogPayload_DeletedMetadata:
			return "DELETE_METADATA", ledgerName
		case *commonpb.LedgerLogPayload_SetMetadataFieldType:
			return "SET_META_FIELD_TYPE", ledgerName
		case *commonpb.LedgerLogPayload_RemovedMetadataFieldType:
			return "REMOVE_META_FIELD_TYPE", ledgerName
		case *commonpb.LedgerLogPayload_ConvertMetadataBatch:
			return "CONVERT_META_BATCH", ledgerName
		case *commonpb.LedgerLogPayload_MetadataConversionComplete:
			return "META_CONVERSION_DONE", ledgerName
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
	case *commonpb.LogPayload_SetPeriodSchedule:
		return "SET_PERIOD_SCHED", ""
	case *commonpb.LogPayload_DeletePeriodSchedule:
		return "DEL_PERIOD_SCHED", ""
	case *commonpb.LogPayload_SetAuditConfig:
		return "SET_AUDIT_CFG", ""
	case *commonpb.LogPayload_PromoteLedger:
		if t.PromoteLedger != nil && t.PromoteLedger.GetInfo() != nil {
			return "PROMOTE_LEDGER", t.PromoteLedger.GetInfo().GetName()
		}

		return "PROMOTE_LEDGER", ""
	default:
		return "UNKNOWN", ""
	}
}

// formatLogDetails returns additional details for a log entry.
func formatLogDetails(log *commonpb.Log) string {
	if log.GetPayload() == nil {
		return ""
	}

	switch t := log.GetPayload().GetType().(type) {
	case *commonpb.LogPayload_Apply:
		if t.Apply == nil || t.Apply.GetLog() == nil {
			return ""
		}

		ledgerLog := t.Apply.GetLog()

		ts := ""
		if ledgerLog.GetDate() != nil {
			ts = ledgerLog.GetDate().AsTime().Format(time.RFC3339)
		}

		switch p := ledgerLog.GetData().GetPayload().(type) {
		case *commonpb.LedgerLogPayload_CreatedTransaction:
			if p.CreatedTransaction != nil && p.CreatedTransaction.GetTransaction() != nil {
				return fmt.Sprintf("tx=%d %s", p.CreatedTransaction.GetTransaction().GetId(), ts)
			}
		case *commonpb.LedgerLogPayload_RevertedTransaction:
			if p.RevertedTransaction != nil {
				return fmt.Sprintf("reverted_tx=%d %s", p.RevertedTransaction.GetRevertedTransactionId(), ts)
			}
		}

		return ts
	case *commonpb.LogPayload_CreateLedger:
		if t.CreateLedger != nil && t.CreateLedger.GetInfo() != nil && t.CreateLedger.GetInfo().GetCreatedAt() != nil {
			return t.CreateLedger.GetInfo().GetCreatedAt().AsTime().Format(time.RFC3339)
		}
	case *commonpb.LogPayload_SetMaintenanceMode:
		if t.SetMaintenanceMode != nil {
			return fmt.Sprintf("enabled=%v", t.SetMaintenanceMode.GetEnabled())
		}
	case *commonpb.LogPayload_SetPeriodSchedule:
		if t.SetPeriodSchedule != nil {
			return "cron=" + t.SetPeriodSchedule.GetCron()
		}
	case *commonpb.LogPayload_SetAuditConfig:
		if t.SetAuditConfig != nil {
			return fmt.Sprintf("enabled=%v", t.SetAuditConfig.GetEnabled())
		}
	}

	return ""
}
