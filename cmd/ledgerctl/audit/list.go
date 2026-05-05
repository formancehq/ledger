package audit

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
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

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Bool("failures-only", false, "Show only failed entries")
	cmd.Flags().Uint64("after", 0, "Show entries after this sequence number")
	cmd.Flags().Uint32("page-size", cmdutil.DefaultPageSize, "Number of entries per page (0 = unlimited)")
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
		failuresOnly, _ = cmd.Flags().GetBool("failures-only")
		after, _        = cmd.Flags().GetUint64("after")
		pageSize, _     = cmd.Flags().GetUint32("page-size")
		minLogSeq, _    = cmd.Flags().GetUint64("min-log-sequence")
	)

	req := &servicepb.ListAuditEntriesRequest{
		FailuresOnly:   failuresOnly,
		PageSize:       pageSize,
		MinLogSequence: minLogSeq,
	}
	if cmd.Flags().Changed("after") {
		req.AfterSequence = &after
	}

	stream, err := client.ListAuditEntries(ctx, req)
	if err != nil {
		return cmdutil.FormatGRPCError("failed to list audit entries", err)
	}

	var entries []*auditpb.AuditEntry

	for {
		entry, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return cmdutil.FormatGRPCError("receiving audit entry", err)
		}

		entries = append(entries, entry)
		if pageSize > 0 && uint32(len(entries)) >= pageSize {
			break
		}
	}

	if handled, err := cmdutil.EncodeStructured(cmd, entries); handled || err != nil {
		return err
	}

	if len(entries) == 0 {
		pterm.Info.Println("No audit entries found.")

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
	if entry.GetTimestamp() != nil {
		ts = entry.GetTimestamp().AsTime().Format(time.RFC3339)
	}

	// Status indicator
	var statusIcon, statusText string
	if entry.GetSuccess() != nil {
		statusIcon = pterm.Green("OK")
		statusText = formatLogSequences(entry.GetSuccess().GetLogSequences())
	} else if entry.GetFailure() != nil {
		f := entry.GetFailure()
		statusIcon = pterm.Red("FAIL")
		statusText = fmt.Sprintf("[%s] %s", f.GetErrorType(), f.GetMessage())
	}

	pterm.Printf("  #%-6d %s  proposal=%-4d %s  %s\n",
		entry.GetSequence(),
		pterm.Gray(ts),
		entry.GetProposalId(),
		statusIcon,
		pterm.Gray(statusText),
	)

	// Group consecutive identical orders for compact display
	printGroupedOrders(entry.GetOrders())
}

// formatLogSequences formats log sequences compactly.
// For small sets: logs=[1 2 3]. For large sets: logs=500 (28487003..28487502).
func formatLogSequences(seqs []uint64) string {
	n := len(seqs)
	if n == 0 {
		return "logs=[]"
	}

	if n <= 5 {
		return fmt.Sprintf("logs=%v", seqs)
	}

	return fmt.Sprintf("logs=%d (%d..%d)", n, seqs[0], seqs[n-1])
}

// orderGroup represents a run of consecutive identical order types.
type orderGroup struct {
	orderType   string
	orderDetail string
	keyStr      string
	count       int
}

// printGroupedOrders groups consecutive identical orders and prints them compactly.
func printGroupedOrders(orders []*raftcmdpb.Order) {
	if len(orders) == 0 {
		return
	}

	var groups []orderGroup

	for _, order := range orders {
		orderType, orderDetail := describeOrder(order)

		keyStr := pterm.Gray("unsigned")
		if sig := order.GetSignature(); sig != nil && sig.GetKeyId() != "" {
			keyStr = pterm.Yellow(sig.GetKeyId())
		}

		// Merge with previous group if same type+detail+key
		if len(groups) > 0 {
			last := &groups[len(groups)-1]
			if last.orderType == orderType && last.orderDetail == orderDetail && last.keyStr == keyStr {
				last.count++

				continue
			}
		}

		groups = append(groups, orderGroup{orderType, orderDetail, keyStr, 1})
	}

	for i, g := range groups {
		prefix := "├─"
		if i == len(groups)-1 {
			prefix = "└─"
		}

		countStr := ""
		if g.count > 1 {
			countStr = fmt.Sprintf(" x%d", g.count)
		}

		if g.orderDetail != "" {
			pterm.Printf("    %s %s%s %s  key=%s\n", prefix, pterm.Cyan(g.orderType), countStr, pterm.Gray(g.orderDetail), g.keyStr)
		} else {
			pterm.Printf("    %s %s%s  key=%s\n", prefix, pterm.Cyan(g.orderType), countStr, g.keyStr)
		}
	}
}

// describeOrder returns a human-readable type and detail string for an order.
func describeOrder(order *raftcmdpb.Order) (string, string) {
	switch {
	case order.GetApply() != nil:
		apply := order.GetApply()
		subType, subDetail := describeApplyOrder(apply)

		return subType, fmt.Sprintf("ledger=%s %s", apply.GetLedger(), subDetail)
	case order.GetCreateLedger() != nil:
		return "CreateLedger", "name=" + order.GetCreateLedger().GetName()
	case order.GetDeleteLedger() != nil:
		return "DeleteLedger", "name=" + order.GetDeleteLedger().GetName()
	case order.GetRegisterSigningKey() != nil:
		return "RegisterSigningKey", "keyId=" + order.GetRegisterSigningKey().GetKeyId()
	case order.GetRevokeSigningKey() != nil:
		return "RevokeSigningKey", "keyId=" + order.GetRevokeSigningKey().GetKeyId()
	case order.GetSetSigningConfig() != nil:
		return "SetSigningConfig", fmt.Sprintf("requireSignatures=%v", order.GetSetSigningConfig().GetRequireSignatures())
	case order.GetAddEventsSink() != nil:
		return "AddEventsSink", ""
	case order.GetRemoveEventsSink() != nil:
		return "RemoveEventsSink", "name=" + order.GetRemoveEventsSink().GetName()
	case order.GetClosePeriod() != nil:
		return "ClosePeriod", ""
	case order.GetSealPeriod() != nil:
		return "SealPeriod", fmt.Sprintf("periodId=%d", order.GetSealPeriod().GetPeriodId())
	case order.GetArchivePeriod() != nil:
		return "ArchivePeriod", fmt.Sprintf("periodId=%d", order.GetArchivePeriod().GetPeriodId())
	case order.GetConfirmArchivePeriod() != nil:
		return "ConfirmArchivePeriod", fmt.Sprintf("periodId=%d", order.GetConfirmArchivePeriod().GetPeriodId())
	case order.GetSetMaintenanceMode() != nil:
		return "SetMaintenanceMode", fmt.Sprintf("enabled=%v", order.GetSetMaintenanceMode().GetEnabled())
	case order.GetSetPeriodSchedule() != nil:
		return "SetPeriodSchedule", "cron=" + order.GetSetPeriodSchedule().GetCron()
	case order.GetDeletePeriodSchedule() != nil:
		return "DeletePeriodSchedule", ""
	case order.GetMirrorIngest() != nil:
		return "MirrorIngest", "ledger=" + order.GetMirrorIngest().GetLedger()
	case order.GetPromoteLedger() != nil:
		return "PromoteLedger", "ledger=" + order.GetPromoteLedger().GetLedger()
	default:
		return "Unknown", ""
	}
}

// describeApplyOrder returns a human-readable sub-type and detail for a LedgerApplyOrder.
func describeApplyOrder(apply *raftcmdpb.LedgerApplyOrder) (string, string) {
	switch {
	case apply.GetCreateTransaction() != nil:
		tx := apply.GetCreateTransaction()
		if tx.GetReference() != "" {
			return "CreateTransaction", "ref=" + tx.GetReference()
		}

		return "CreateTransaction", ""
	case apply.GetAddMetadata() != nil:
		return "AddMetadata", ""
	case apply.GetRevertTransaction() != nil:
		return "RevertTransaction", ""
	case apply.GetDeleteMetadata() != nil:
		return "DeleteMetadata", ""
	case apply.GetSetMetadataFieldType() != nil:
		o := apply.GetSetMetadataFieldType()

		return "SetMetadataFieldType", fmt.Sprintf("target=%s key=%s type=%s", o.GetTargetType(), o.GetKey(), o.GetType())
	case apply.GetRemoveMetadataFieldType() != nil:
		o := apply.GetRemoveMetadataFieldType()

		return "RemoveMetadataFieldType", fmt.Sprintf("target=%s key=%s", o.GetTargetType(), o.GetKey())
	case apply.GetConvertMetadataBatch() != nil:
		o := apply.GetConvertMetadataBatch()

		return "ConvertMetadataBatch", fmt.Sprintf("target=%s key=%s %d/%d", o.GetTargetType(), o.GetKey(), o.GetConvertedKeysSoFar(), o.GetTotalKeys())
	case apply.GetConversionComplete() != nil:
		o := apply.GetConversionComplete()

		return "ConversionComplete", fmt.Sprintf("target=%s key=%s type=%s", o.GetTargetType(), o.GetKey(), o.GetExpectedType())
	default:
		return "Apply", ""
	}
}
