package audit

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewListCommand creates the audit list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "list",
		Aliases:           cmdutil.ListAliases,
		Short:             "List audit entries",
		Long:              "List audit log entries (successes and failures) via gRPC streaming",
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runList,
	}

	cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{})
	cmdutil.AddMinLogSequenceFlag(cmd)
	cmdutil.AddOutputFlags(cmd)
	cmdutil.AddFilterFlags(cmd, cmdutil.FilterOptions{})
	cmd.Flags().Bool("failures-only", false, "Shorthand for --filter 'audit[outcome] == failure'")
	cmd.Flags().String("ledger", "", "Shorthand for --filter 'audit[ledger] == <name>'")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	cmd.Flags().Bool("expand", false, "Expand orders within each audit entry")

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

	failuresOnly, _ := cmd.Flags().GetBool("failures-only")
	ledger, _ := cmd.Flags().GetString("ledger")
	expand, _ := cmd.Flags().GetBool("expand")
	minLogSeq, _ := cmd.Flags().GetUint64("min-log-sequence")
	pgn := cmdutil.GetPaginationFlags(cmd)
	ff := cmdutil.GetFilterFlags(cmd)

	filter, err := buildAuditFilter(ff.Expr, failuresOnly, ledger)
	if err != nil {
		return err
	}

	stream, err := client.ListAuditEntries(ctx, &servicepb.ListAuditEntriesRequest{
		Options: cmdutil.BuildListOptions(pgn, cmdutil.ConsistencyFlags{MinLogSequence: minLogSeq}, filter),
	})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to list audit entries", err)
	}

	// Drain the stream to EOF so the gRPC trailer (x-next-cursor) is
	// available — the server already caps the stream at pageSize, so reading
	// to EOF is the canonical way to consume one page.
	entries, err := cmdutil.CollectStream(stream)
	if err != nil {
		return cmdutil.FormatGRPCError("receiving audit entry", err)
	}

	nextCursor := cmdutil.NextCursorFromTrailer(stream.Trailer())

	if expand {
		for i, entry := range entries {
			full, err := client.GetAuditEntry(ctx, &servicepb.GetAuditEntryRequest{
				Sequence: entry.GetSequence(),
			})
			if err != nil {
				return cmdutil.FormatGRPCError("expanding audit entry", err)
			}

			entries[i] = full
		}
	}

	if handled, err := cmdutil.EncodeStructured(cmd, entries); handled || err != nil {
		// Surface the resume cursor to stderr so --json/--yaml output stays a
		// pure payload on stdout while scripts can still grep stderr for it.
		cmdutil.EmitNextCursorHint(cmd, nextCursor)

		return err
	}

	if len(entries) == 0 {
		pterm.Info.Println("No audit entries found.")

		return nil
	}

	for _, entry := range entries {
		printAuditEntry(entry, expand)
	}

	pterm.Println()
	pterm.Info.Printfln("%d audit entry(ies) displayed", len(entries))

	cmdutil.EmitNextCursorHint(cmd, nextCursor)

	return nil
}

// buildAuditFilter combines --filter with the --failures-only / --ledger
// shorthands into a single QueryFilter (AND-combined). Returns nil when empty.
func buildAuditFilter(expr string, failuresOnly bool, ledger string) (*commonpb.QueryFilter, error) {
	var parts []*commonpb.QueryFilter

	if expr != "" {
		parsed, err := filterexpr.Parse(expr)
		if err != nil {
			return nil, fmt.Errorf("invalid filter expression: %w", err)
		}
		parts = append(parts, parsed)
	}
	if failuresOnly {
		f, err := filterexpr.Parse(`audit[outcome] == failure`)
		if err != nil {
			return nil, err
		}
		parts = append(parts, f)
	}
	if ledger != "" {
		// Build the proto directly instead of round-tripping through the DSL
		// string. The filterexpr String grammar has no escape handling, so a
		// ledger name containing a quote or backslash (both valid per
		// domain.ValidateLedgerName) cannot survive
		// Parse(fmt.Sprintf("audit[ledger] == %q", ledger)).
		parts = append(parts, &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{
				Field: commonpb.AuditField_AUDIT_FIELD_LEDGER,
				Condition: &commonpb.AuditCondition_StringCond{
					StringCond: &commonpb.StringCondition{
						Value: &commonpb.StringCondition_Hardcoded{Hardcoded: ledger},
					},
				},
			}},
		})
	}

	switch len(parts) {
	case 0:
		return nil, nil
	case 1:
		return parts[0], nil
	default:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: parts}}}, nil
	}
}

// printAuditEntry prints a single audit entry in a human-readable format.
func printAuditEntry(entry *auditpb.AuditEntry, verbose bool) {
	ts := "-"
	if entry.GetTimestamp() != nil {
		ts = entry.GetTimestamp().AsTime().Format(time.RFC3339)
	}

	// Status indicator
	var statusIcon, statusText string
	if entry.GetSuccess() != nil {
		statusIcon = pterm.Green("OK")
		statusText = formatLogRange(entry.GetSuccess().GetMinLogSequence(), entry.GetSuccess().GetMaxLogSequence())
	} else if entry.GetFailure() != nil {
		f := entry.GetFailure()
		statusIcon = pterm.Red("FAIL")
		statusText = fmt.Sprintf("[%s] %s", domain.ReasonString(f.GetReason()), f.GetMessage())
	}

	// Caller info (compact or verbose)
	callerText := ""
	if id := entry.GetCallerSnapshot().GetIdentity(); id.GetSubject() != "" {
		callerText = "  caller=" + pterm.Yellow(id.GetSubject())
	}

	pterm.Printf("  #%-6d %s  proposal=%-4d %s  %s%s\n",
		entry.GetSequence(),
		pterm.Gray(ts),
		entry.GetProposalId(),
		statusIcon,
		pterm.Gray(statusText),
		callerText,
	)

	// Verbose caller details
	if verbose {
		if snap := entry.GetCallerSnapshot(); snap != nil && snap.GetIdentity().GetSubject() != "" {
			id := snap.GetIdentity()

			var source string

			switch s := id.GetSource().(type) {
			case *commonpb.CallerIdentity_Issuer:
				source = "issuer=" + s.Issuer
			case *commonpb.CallerIdentity_KeyId:
				source = "key_id=" + s.KeyId
			}

			if snap.GetGod() {
				pterm.Printf("    %s subject=%s %s %s\n",
					pterm.Gray("caller:"),
					pterm.Yellow(id.GetSubject()),
					pterm.Gray(source),
					pterm.Red("god=true"),
				)
			} else {
				pterm.Printf("    %s subject=%s %s scopes=[%s]\n",
					pterm.Gray("caller:"),
					pterm.Yellow(id.GetSubject()),
					pterm.Gray(source),
					pterm.Gray(strings.Join(snap.GetScopes(), ",")),
				)
			}
		}
	}

	// Display items if populated (GetAuditEntry), otherwise show order count summary.
	if items := entry.GetItems(); len(items) > 0 {
		// AuditItem stores the deterministic order bytes (what the hash
		// chain is computed over). For display we unmarshal them back
		// into Order — best effort, since proto evolution is forward-
		// and backward-compatible at the unmarshal level. A failure
		// here is a display issue only; the hash chain itself is intact.
		orders := make([]*raftcmdpb.Order, 0, len(items))
		for _, item := range items {
			order := &raftcmdpb.Order{}
			if err := order.UnmarshalVT(item.GetSerializedOrder()); err != nil {
				pterm.Printf("    %s order index=%d: %s\n",
					pterm.Yellow("⚠"),
					item.GetOrderIndex(),
					pterm.Red(fmt.Sprintf("unable to decode (%s)", err)),
				)

				continue
			}

			orders = append(orders, order)
		}

		printGroupedOrders(orders, verbose)
	} else if entry.GetOrderCount() > 0 {
		pterm.Printf("    └─ %s orders\n", pterm.Cyan(strconv.FormatUint(uint64(entry.GetOrderCount()), 10)))
	}
}

// formatLogRange formats a log sequence range compactly.
func formatLogRange(minSeq, maxSeq uint64) string {
	if minSeq == 0 && maxSeq == 0 {
		return "logs=[]"
	}

	if minSeq == maxSeq {
		return fmt.Sprintf("logs=[%d]", minSeq)
	}

	return fmt.Sprintf("logs=%d..%d", minSeq, maxSeq)
}

// orderGroup represents a run of consecutive identical order types.
type orderGroup struct {
	cmdutil.OneofDescription

	keyStr string
	count  int
}

// printGroupedOrders groups consecutive identical orders and prints them compactly.
func printGroupedOrders(orders []*raftcmdpb.Order, verbose bool) {
	if len(orders) == 0 {
		return
	}

	var groups []orderGroup

	for _, order := range orders {
		desc := describeOrder(order, verbose)

		// Signatures are batch-level now (carried on the Log / audit entry), not
		// per order, so the per-order grouping no longer surfaces a signing key.
		keyStr := pterm.Gray("unsigned")

		// Merge with previous group if same type+detail+key and no map lines
		if len(groups) > 0 && len(desc.MapLines) == 0 {
			last := &groups[len(groups)-1]
			if last.Type == desc.Type && last.Detail == desc.Detail && last.keyStr == keyStr && len(last.MapLines) == 0 {
				last.count++

				continue
			}
		}

		groups = append(groups, orderGroup{desc, keyStr, 1})
	}

	for i, g := range groups {
		prefix := "├─"
		if i == len(groups)-1 && len(g.MapLines) == 0 {
			prefix = "└─"
		}

		countStr := ""
		if g.count > 1 {
			countStr = fmt.Sprintf(" x%d", g.count)
		}

		if g.Detail != "" {
			pterm.Printf("    %s %s%s %s  key=%s\n", prefix, pterm.Cyan(g.Type), countStr, pterm.Gray(g.Detail), g.keyStr)
		} else {
			pterm.Printf("    %s %s%s  key=%s\n", prefix, pterm.Cyan(g.Type), countStr, g.keyStr)
		}

		if len(g.MapLines) > 0 {
			// Find max key length for alignment.
			maxKeyLen := 0
			for _, kv := range g.MapLines {
				if len(kv[0]) > maxKeyLen {
					maxKeyLen = len(kv[0])
				}
			}

			for j, kv := range g.MapLines {
				linePrefix := "│  "
				if i == len(groups)-1 {
					linePrefix = "   "
				}
				bullet := "├─"
				if j == len(g.MapLines)-1 {
					bullet = "└─"
				}
				pterm.Printf("    %s %s %s %s %s\n",
					pterm.Gray(linePrefix), pterm.Gray(bullet),
					pterm.Yellow(fmt.Sprintf("%-*s", maxKeyLen, kv[0])),
					pterm.Gray("="),
					kv[1],
				)
			}
		}
	}
}

// describeOrder returns the display description for a single order. For an
// Apply payload the description bubbles up the inner data oneof (so the user
// sees "create_transaction" rather than "apply") and prepends the ledger name
// carried by the LedgerScopedOrder wrapper.
func describeOrder(order *raftcmdpb.Order, verbose bool) cmdutil.OneofDescription {
	if ls := order.GetLedgerScoped(); ls != nil {
		if apply := ls.GetApply(); apply != nil {
			desc := cmdutil.DescribeOneofField(apply.ProtoReflect(), "data", "LedgerScopedOrder", verbose)
			desc.PrependField("ledger", ls.GetLedger())

			return desc
		}

		desc := cmdutil.DescribeOneofField(ls.ProtoReflect(), "payload", "LedgerScopedOrder", verbose)
		desc.PrependField("ledger", ls.GetLedger())

		return desc
	}

	if ss := order.GetSystemScoped(); ss != nil {
		return cmdutil.DescribeOneofField(ss.ProtoReflect(), "payload", "SystemScopedOrder", verbose)
	}

	return cmdutil.DescribeOneofField(order.ProtoReflect(), "type", "Order", verbose)
}
