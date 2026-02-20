package audit

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
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
	cmd.Flags().String("ledger", "", "Filter by ledger name")
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
		ledger, _       = cmd.Flags().GetString("ledger")
		after, _        = cmd.Flags().GetUint64("after")
		limit, _        = cmd.Flags().GetInt("limit")
	)

	req := &servicepb.ListAuditEntriesRequest{
		Ledger:       ledger,
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

	// Extract ledger names from orders
	ledgers := extractLedgerNames(entry)
	ledgerStr := ""
	if len(ledgers) > 0 {
		ledgerStr = fmt.Sprintf(" ledger=%s", pterm.Cyan(strings.Join(ledgers, ",")))
	}

	pterm.Printf("  #%-6d %s  proposal=%-4d %s%s  %s\n",
		entry.Sequence,
		pterm.Gray(ts),
		entry.ProposalId,
		statusIcon,
		ledgerStr,
		pterm.Gray(statusText),
	)
}

// extractLedgerNames extracts unique ledger names from audit entry orders.
func extractLedgerNames(entry *auditpb.AuditEntry) []string {
	seen := make(map[string]struct{})
	var names []string

	for _, order := range entry.Orders {
		var name string
		if apply := order.GetApply(); apply != nil {
			name = apply.Ledger
		} else if create := order.GetCreateLedger(); create != nil {
			name = create.Name
		} else if del := order.GetDeleteLedger(); del != nil {
			name = del.Name
		}
		if name != "" {
			if _, ok := seen[name]; !ok {
				seen[name] = struct{}{}
				names = append(names, name)
			}
		}
	}

	return names
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
