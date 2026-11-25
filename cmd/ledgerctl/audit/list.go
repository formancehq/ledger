package audit

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewListCommand creates the audit list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: cmdutil.ListAliases,
		Short:   "List audit entries",
		Long: `List audit log entries (successes and failures) via gRPC streaming.

Entries are shown oldest first by default (chronological); use --reverse for newest first.

Filtering is done entirely through the generic --filter flag (same DSL as the
other list commands); there are no audit-specific flags. On the audit endpoint
the audit fields are bare (no audit[...] prefix), combined with and/or:
  outcome == failure
  ledger == main
  ledger == main and order_type == create_transaction
  order_type in (create_transaction, revert_transaction)
  caller_subject == "svc:payments"
  seq between 1000 and 2000
  proposal_id == 42
  timestamp >= "2023-11-14T22:13:20Z"    # RFC3339 or raw unix microseconds
  log_seq == 500
  idempotency_key == "payment-42"
  idempotency_key ^= "import-2026-"    # index-backed prefix match

Supported fields: seq, proposal_id, log_seq (numeric); timestamp
(RFC3339 or unix microseconds); outcome (success|failure), ledger,
caller_subject, order_type, idempotency_key (string). Only idempotency_key
supports the ^= prefix operator; a reused key returns every audit entry
recorded under it.
Unsupported conditions (not, non-indexed fields) are rejected. The JSON DSL
takes the same bare fields, e.g. '{"$match":{"outcome":"failure"}}', without
the ^= prefix operator.

Examples:
  ledgerctl audit list
  ledgerctl audit list --reverse
  ledgerctl audit list --filter 'outcome == failure'
  ledgerctl audit list --filter 'ledger == "my-ledger"'
  ledgerctl audit list --filter 'idempotency_key == "payment-42"'
  ledgerctl audit list --filter '{"$match":{"outcome":"failure"}}'
  ledgerctl audit list --checkpoint-id 7`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runList,
	}

	cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{SupportsReverse: true, SupportsAll: true})
	cmdutil.AddFilterFlags(cmd, cmdutil.FilterOptions{
		FilterHelp: `Filter expression: textual grammar ("outcome == failure") or JSON DSL ('{"$match":{"outcome":"failure"}}')`,
	})
	cmdutil.AddConsistencyFlags(cmd)
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	cmd.Flags().Bool("expand", false, "Expand orders within each audit entry")
	cmd.Flags().Bool("decode-orders", false, "Include decoded order JSON with expanded structured audit entries")

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

	expand, _ := cmd.Flags().GetBool("expand")
	decodeOrders, _ := cmd.Flags().GetBool("decode-orders")
	jsonOutput, _ := cmd.Flags().GetBool("json")
	yamlOutput, _ := cmd.Flags().GetBool("yaml")
	if decodeOrders && (!expand || (!jsonOutput && !yamlOutput)) {
		return errors.New("--decode-orders requires --expand and --json or --yaml")
	}
	pgn := cmdutil.GetPaginationFlags(cmd)
	flt := cmdutil.GetFilterFlags(cmd)
	cns := cmdutil.GetConsistencyFlags(cmd)

	filter, err := cmdutil.BuildQueryFilter(flt.Expr, flt.Prefix, commonpb.QueryTarget_QUERY_TARGET_AUDIT)
	if err != nil {
		return err
	}

	fetchPage := func(cursor string) ([]*auditpb.AuditEntry, metadata.MD, error) {
		options := cmdutil.BuildListOptions(pgn, cns, filter)
		options.Cursor = cursor
		stream, err := client.ListAuditEntries(ctx, &servicepb.ListAuditEntriesRequest{Options: options})
		if err != nil {
			return nil, nil, cmdutil.FormatGRPCError("failed to list audit entries", err)
		}
		entries, err := cmdutil.CollectStream(stream)
		if err != nil {
			return nil, nil, cmdutil.FormatGRPCError("receiving audit entry", err)
		}

		return entries, stream.Trailer(), nil
	}
	var entries []*auditpb.AuditEntry
	var trailer metadata.MD
	if pgn.All {
		entries, err = cmdutil.DrainAllPages(pgn.Cursor, fetchPage)
	} else {
		entries, trailer, err = fetchPage(pgn.Cursor)
	}
	if err != nil {
		return err
	}
	cursors := cmdutil.CursorsFromTrailer(trailer)

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

	var output any = entries
	if decodeOrders {
		decoded, err := decodeAuditEntries(entries)
		if err != nil {
			return err
		}
		output = decoded
	}
	if handled, err := cmdutil.EncodeStructured(cmd, output); handled || err != nil {
		// Surface the resume cursor to stderr so --json/--yaml output stays a
		// pure payload on stdout while scripts can still grep stderr for it.
		cmdutil.EmitCursorHints(cmd, cursors)

		return err
	}

	if len(entries) == 0 {
		pterm.Info.Println("No audit entries found.")
		cmdutil.EmitCursorHints(cmd, cursors)

		return nil
	}

	for _, entry := range entries {
		printAuditEntry(entry, expand)
	}

	pterm.Println()
	pterm.Info.Printfln("%d audit entry(ies) displayed", len(entries))

	cmdutil.EmitCursorHints(cmd, cursors)

	return nil
}

type decodedAuditEntry struct {
	Entry  json.RawMessage   `json:"entry"`
	Orders []json.RawMessage `json:"orders"`
}

func decodeAuditEntries(entries []*auditpb.AuditEntry) ([]decodedAuditEntry, error) {
	decoded := make([]decodedAuditEntry, 0, len(entries))
	for _, entry := range entries {
		body, err := protojson.Marshal(entry)
		if err != nil {
			return nil, fmt.Errorf("encoding audit entry %d: %w", entry.GetSequence(), err)
		}
		item := decodedAuditEntry{Entry: body}
		for _, auditItem := range entry.GetItems() {
			order := &raftcmdpb.Order{}
			if err := order.UnmarshalVT(auditItem.GetSerializedOrder()); err != nil {
				return nil, fmt.Errorf("decoding order %d in audit entry %d: %w", auditItem.GetOrderIndex(), entry.GetSequence(), err)
			}
			body, err := protojson.Marshal(order)
			if err != nil {
				return nil, fmt.Errorf("encoding order %d in audit entry %d: %w", auditItem.GetOrderIndex(), entry.GetSequence(), err)
			}
			item.Orders = append(item.Orders, body)
		}
		decoded = append(decoded, item)
	}

	return decoded, nil
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
		message, _ := domain.RenderFailureFacts(domain.FailureFacts{
			Reason: f.GetReason(), Code: f.GetCode(), Facts: f.GetFacts(),
		})
		statusIcon = pterm.Red("FAIL")
		statusText = fmt.Sprintf("[%s] %s", domain.ReasonString(f.GetReason()), message)
	}

	// Caller info (compact or verbose)
	callerText := ""
	if label := callerLabel(entry.GetCallerSnapshot()); label != "" {
		callerText = "  caller=" + pterm.Yellow(label)
	}

	signatureText := ""
	if !verbose {
		signatureText = "  " + signatureLabel(entry)
	}

	pterm.Printf("  #%-6d %s  proposal=%-4d %s  %s%s%s\n",
		entry.GetSequence(),
		pterm.Gray(ts),
		entry.GetProposalId(),
		statusIcon,
		pterm.Gray(statusText),
		callerText,
		signatureText,
	)

	// Verbose caller details
	if verbose {
		if snap := entry.GetCallerSnapshot(); snap != nil {
			switch principal := snap.GetPrincipal().(type) {
			case *commonpb.CallerSnapshot_Authenticated:
				caller := principal.Authenticated
				id := caller.GetIdentity()
				subject := id.GetSubject()
				if subject == "" {
					subject = "(none)"
				}
				if caller.GetSuperuser() {
					pterm.Printf("    %s authenticated subject=%s %s %s\n",
						pterm.Gray("caller:"),
						pterm.Yellow(subject),
						pterm.Gray(callerSourceString(id)),
						pterm.Red("superuser=true"),
					)
				} else {
					pterm.Printf("    %s authenticated subject=%s %s scopes=[%s]\n",
						pterm.Gray("caller:"),
						pterm.Yellow(subject),
						pterm.Gray(callerSourceString(id)),
						pterm.Gray(strings.Join(caller.GetScopes(), ",")),
					)
				}
			case *commonpb.CallerSnapshot_Anonymous:
				pterm.Printf("    %s anonymous scopes=[%s]\n",
					pterm.Gray("caller:"),
					pterm.Gray(strings.Join(principal.Anonymous.GetScopes(), ",")),
				)
			case *commonpb.CallerSnapshot_System:
				pterm.Printf("    %s subject=%s %s %s\n",
					pterm.Gray("caller:"),
					pterm.Yellow("(none)"),
					pterm.Gray("system="+principal.System.GetComponent()),
					pterm.Gray("scopes=[]"),
				)
			case *commonpb.CallerSnapshot_AuthDisabled:
				pterm.Printf("    %s authentication-disabled\n", pterm.Gray("caller:"))
			}
		}
	}

	if verbose {
		printBatchIdentity(entry)
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

// signatureLabel renders the batch signing key, or "unsigned".
func signatureLabel(entry *auditpb.AuditEntry) string {
	if sig := entry.GetSignature(); sig != nil {
		return "signature=" + pterm.Green(sig.GetKeyId())
	}

	return pterm.Gray("unsigned")
}

// printBatchIdentity prints the batch-level signature and idempotency key.
func printBatchIdentity(entry *auditpb.AuditEntry) {
	line := signatureLabel(entry)

	if key := entry.GetIdempotency().GetKey(); key != "" {
		line += "  idempotency=" + pterm.Yellow(sanitizeDisplayText(key))
	}

	pterm.Printf("    %s\n", line)
}

// sanitizeDisplayText replaces control characters in client-supplied text so
// it cannot inject escape sequences or forged lines into the terminal.
func sanitizeDisplayText(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f) {
			return '.'
		}

		return r
	}, s)
}

// callerSourceString renders an authenticated identity source for display.
func callerSourceString(id *commonpb.CallerIdentity) string {
	switch s := id.GetSource().(type) {
	case *commonpb.CallerIdentity_Issuer:
		return "issuer=" + s.Issuer
	case *commonpb.CallerIdentity_KeyId:
		return "key_id=" + s.KeyId
	default:
		return ""
	}
}

// callerLabel renders a compact one-token caller label, falling back to the
// credential source when an authenticated subject is empty.
func callerLabel(snap *commonpb.CallerSnapshot) string {
	if snap == nil {
		return ""
	}

	switch principal := snap.GetPrincipal().(type) {
	case *commonpb.CallerSnapshot_Authenticated:
		id := principal.Authenticated.GetIdentity()
		if id.GetSubject() != "" {
			return id.GetSubject()
		}
		switch source := id.GetSource().(type) {
		case *commonpb.CallerIdentity_KeyId:
			return "key:" + source.KeyId
		case *commonpb.CallerIdentity_Issuer:
			return "issuer:" + source.Issuer
		default:
			return "authenticated"
		}
	case *commonpb.CallerSnapshot_Anonymous:
		return "anonymous"
	case *commonpb.CallerSnapshot_System:
		return "system:" + principal.System.GetComponent()
	case *commonpb.CallerSnapshot_AuthDisabled:
		return "auth-disabled"
	default:
		return ""
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

	count int
}

// printGroupedOrders groups consecutive identical orders and prints them compactly.
func printGroupedOrders(orders []*raftcmdpb.Order, verbose bool) {
	if len(orders) == 0 {
		return
	}

	var groups []orderGroup

	for _, order := range orders {
		desc := describeOrder(order, verbose)

		// Merge with previous group if same type+detail and no map lines
		if len(groups) > 0 && len(desc.MapLines) == 0 {
			last := &groups[len(groups)-1]
			if last.Type == desc.Type && last.Detail == desc.Detail && len(last.MapLines) == 0 {
				last.count++

				continue
			}
		}

		groups = append(groups, orderGroup{desc, 1})
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
			pterm.Printf("    %s %s%s %s\n", prefix, pterm.Cyan(g.Type), countStr, pterm.Gray(g.Detail))
		} else {
			pterm.Printf("    %s %s%s\n", prefix, pterm.Cyan(g.Type), countStr)
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
