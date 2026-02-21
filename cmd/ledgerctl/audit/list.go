package audit

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

// NewListCommand creates the audit list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "l"},
		Short:   "List audit entries",
		Long:    "List audit log entries (successes and failures) via gRPC streaming",
		RunE:    runList,
	}

	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Bool("failures-only", false, "Show only failed entries")
	cmd.Flags().Uint64("after", 0, "Show entries after this sequence number")
	cmd.Flags().Int("limit", 0, "Maximum number of entries to display (0 = unlimited)")
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
		jsonOutput, _   = cmd.Flags().GetBool("json")
		failuresOnly, _ = cmd.Flags().GetBool("failures-only")
		after, _        = cmd.Flags().GetUint64("after")
		limit, _        = cmd.Flags().GetInt("limit")
	)

	req := &servicepb.ListAuditEntriesRequest{
		FailuresOnly: failuresOnly,
	}
	if cmd.Flags().Changed("after") {
		req.AfterSequence = &after
	}

	stream, err := client.ListAuditEntries(ctx, req)
	if err != nil {
		if isAuditDisabledError(err) {
			pterm.Warning.Println("Audit log is disabled on this server.")
			pterm.Println(pterm.Gray("Start the server with --audit-enabled=true to enable audit logging."))
			return nil
		}
		return cmdutil.FormatGRPCError("failed to list audit entries", err)
	}

	var entries []*auditpb.AuditEntry
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if isAuditDisabledError(err) {
				pterm.Warning.Println("Audit log is disabled on this server.")
				pterm.Println(pterm.Gray("Start the server with --audit-enabled=true to enable audit logging."))
				return nil
			}
			return cmdutil.FormatGRPCError("receiving audit entry", err)
		}
		entries = append(entries, entry)
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
		pterm.Println("No audit entries found.")
		return nil
	}

	for _, entry := range entries {
		printAuditEntry(entry)
	}

	pterm.Println()
	pterm.Info.Printfln("%d audit entry(ies) displayed", len(entries))

	return nil
}

// printAuditEntry prints a single audit entry in a human-readable format.
func printAuditEntry(entry *auditpb.AuditEntry) {
	ts := "-"
	if entry.Timestamp != nil {
		ts = entry.Timestamp.AsTime().Format(time.RFC3339)
	}

	// Status indicator
	var statusIcon, statusText string
	if entry.GetSuccess() != nil {
		statusIcon = pterm.Green("OK")
		statusText = fmt.Sprintf("logs=%v", entry.GetSuccess().LogSequences)
	} else if entry.GetFailure() != nil {
		f := entry.GetFailure()
		statusIcon = pterm.Red("FAIL")
		statusText = fmt.Sprintf("[%s] %s", f.ErrorType, f.Message)
	}

	pterm.Printf("  #%-6d %s  proposal=%-4d %s  %s\n",
		entry.Sequence,
		pterm.Gray(ts),
		entry.ProposalId,
		statusIcon,
		pterm.Gray(statusText),
	)

	// Print each order with its details and signing key
	for i, order := range entry.Orders {
		orderType, orderDetail := describeOrder(order)

		keyStr := pterm.Gray("unsigned")
		if sig := order.GetSignature(); sig != nil && sig.KeyId != "" {
			keyStr = pterm.Yellow(sig.KeyId)
		}

		prefix := "├─"
		if i == len(entry.Orders)-1 {
			prefix = "└─"
		}

		if orderDetail != "" {
			pterm.Printf("    %s %s %s  key=%s\n", prefix, pterm.Cyan(orderType), pterm.Gray(orderDetail), keyStr)
		} else {
			pterm.Printf("    %s %s  key=%s\n", prefix, pterm.Cyan(orderType), keyStr)
		}
	}
}

// describeOrder returns a human-readable type and detail string for an order.
func describeOrder(order *raftcmdpb.Order) (string, string) {
	switch {
	case order.GetApply() != nil:
		apply := order.GetApply()
		subType, subDetail := describeApplyOrder(apply)
		return subType, fmt.Sprintf("ledger=%s %s", apply.Ledger, subDetail)
	case order.GetCreateLedger() != nil:
		return "CreateLedger", fmt.Sprintf("name=%s", order.GetCreateLedger().Name)
	case order.GetDeleteLedger() != nil:
		return "DeleteLedger", fmt.Sprintf("name=%s", order.GetDeleteLedger().Name)
	case order.GetRegisterSigningKey() != nil:
		return "RegisterSigningKey", fmt.Sprintf("keyId=%s", order.GetRegisterSigningKey().KeyId)
	case order.GetRevokeSigningKey() != nil:
		return "RevokeSigningKey", fmt.Sprintf("keyId=%s", order.GetRevokeSigningKey().KeyId)
	case order.GetSetSigningConfig() != nil:
		return "SetSigningConfig", fmt.Sprintf("requireSignatures=%v", order.GetSetSigningConfig().RequireSignatures)
	case order.GetAddEventsSink() != nil:
		return "AddEventsSink", ""
	case order.GetRemoveEventsSink() != nil:
		return "RemoveEventsSink", fmt.Sprintf("name=%s", order.GetRemoveEventsSink().Name)
	case order.GetClosePeriod() != nil:
		return "ClosePeriod", ""
	case order.GetSealPeriod() != nil:
		return "SealPeriod", fmt.Sprintf("periodId=%d", order.GetSealPeriod().PeriodId)
	case order.GetArchivePeriod() != nil:
		return "ArchivePeriod", fmt.Sprintf("periodId=%d", order.GetArchivePeriod().PeriodId)
	case order.GetConfirmArchivePeriod() != nil:
		return "ConfirmArchivePeriod", fmt.Sprintf("periodId=%d", order.GetConfirmArchivePeriod().PeriodId)
	case order.GetSetMaintenanceMode() != nil:
		return "SetMaintenanceMode", fmt.Sprintf("enabled=%v", order.GetSetMaintenanceMode().Enabled)
	case order.GetSetPeriodSchedule() != nil:
		return "SetPeriodSchedule", fmt.Sprintf("cron=%s", order.GetSetPeriodSchedule().Cron)
	case order.GetDeletePeriodSchedule() != nil:
		return "DeletePeriodSchedule", ""
	default:
		return "Unknown", ""
	}
}

// describeApplyOrder returns a human-readable sub-type and detail for a LedgerApplyOrder.
func describeApplyOrder(apply *raftcmdpb.LedgerApplyOrder) (string, string) {
	switch {
	case apply.GetCreateTransaction() != nil:
		tx := apply.GetCreateTransaction()
		if tx.Reference != "" {
			return "CreateTransaction", fmt.Sprintf("ref=%s", tx.Reference)
		}
		return "CreateTransaction", ""
	case apply.GetAddMetadata() != nil:
		return "AddMetadata", ""
	case apply.GetRevertTransaction() != nil:
		return "RevertTransaction", ""
	case apply.GetDeleteMetadata() != nil:
		return "DeleteMetadata", ""
	default:
		return "Apply", ""
	}
}

// isAuditDisabledError checks if a gRPC error indicates that audit is disabled.
func isAuditDisabledError(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}
		if info.Reason == processing.ErrReasonAuditDisabled && info.Domain == "ledger" {
			return true
		}
	}
	return false
}
