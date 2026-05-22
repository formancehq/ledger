package audit

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/reflect/protoreflect"

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

	var (
		failuresOnly, _ = cmd.Flags().GetBool("failures-only")
		after, _        = cmd.Flags().GetUint64("after")
		pageSize, _     = cmd.Flags().GetUint32("page-size")
		minLogSeq, _    = cmd.Flags().GetUint64("min-log-sequence")
		expand, _       = cmd.Flags().GetBool("expand")
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

	return nil
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
		statusText = fmt.Sprintf("[%s] %s", f.GetErrorType(), f.GetMessage())
	}

	pterm.Printf("  #%-6d %s  proposal=%-4d %s  %s\n",
		entry.GetSequence(),
		pterm.Gray(ts),
		entry.GetProposalId(),
		statusIcon,
		pterm.Gray(statusText),
	)

	// Display items if populated (GetAuditEntry), otherwise show order count summary.
	if items := entry.GetItems(); len(items) > 0 {
		orders := make([]*raftcmdpb.Order, len(items))
		for i, item := range items {
			orders[i] = item.GetOrder()
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

// orderDescription holds the display info extracted from a single order.
type orderDescription struct {
	orderType string
	detail    string     // scalar fields on the main line
	mapLines  [][2]string // key/value pairs for indented multi-line display
}

// orderGroup represents a run of consecutive identical order types.
type orderGroup struct {
	orderDescription
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

		keyStr := pterm.Gray("unsigned")
		if sig := order.GetSignature(); sig != nil && sig.GetKeyId() != "" {
			keyStr = pterm.Yellow(sig.GetKeyId())
		}

		// Merge with previous group if same type+detail+key and no map lines
		if len(groups) > 0 && len(desc.mapLines) == 0 {
			last := &groups[len(groups)-1]
			if last.orderType == desc.orderType && last.detail == desc.detail && last.keyStr == keyStr && len(last.mapLines) == 0 {
				last.count++

				continue
			}
		}

		groups = append(groups, orderGroup{desc, keyStr, 1})
	}

	for i, g := range groups {
		prefix := "├─"
		if i == len(groups)-1 && len(g.mapLines) == 0 {
			prefix = "└─"
		}

		countStr := ""
		if g.count > 1 {
			countStr = fmt.Sprintf(" x%d", g.count)
		}

		if g.detail != "" {
			pterm.Printf("    %s %s%s %s  key=%s\n", prefix, pterm.Cyan(g.orderType), countStr, pterm.Gray(g.detail), g.keyStr)
		} else {
			pterm.Printf("    %s %s%s  key=%s\n", prefix, pterm.Cyan(g.orderType), countStr, g.keyStr)
		}

		if len(g.mapLines) > 0 {
			// Find max key length for alignment.
			maxKeyLen := 0
			for _, kv := range g.mapLines {
				if len(kv[0]) > maxKeyLen {
					maxKeyLen = len(kv[0])
				}
			}

			for j, kv := range g.mapLines {
				linePrefix := "│  "
				if i == len(groups)-1 {
					linePrefix = "   "
				}
				bullet := "├─"
				if j == len(g.mapLines)-1 {
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

// describeOrder returns the display description for a single order.
func describeOrder(order *raftcmdpb.Order, verbose bool) orderDescription {
	if apply := order.GetApply(); apply != nil {
		desc := describeOneofField(apply.ProtoReflect(), "data", verbose)
		if desc.detail != "" {
			desc.detail = "ledger=" + apply.GetLedger() + " " + desc.detail
		} else {
			desc.detail = "ledger=" + apply.GetLedger()
		}

		return desc
	}

	return describeOneofField(order.ProtoReflect(), "type", verbose)
}

// describeOneofField uses protobuf reflection to extract the active oneof
// variant name, its scalar fields, and string map entries. This avoids maintaining
// a switch that must be updated every time a new order type is added to the proto.
func describeOneofField(msg protoreflect.Message, oneofName protoreflect.Name, verbose bool) orderDescription {
	od := msg.Descriptor().Oneofs().ByName(oneofName)
	if od == nil {
		return orderDescription{orderType: "Unknown"}
	}

	fd := msg.WhichOneof(od)
	if fd == nil {
		return orderDescription{orderType: "Unknown"}
	}

	typeName := strings.TrimSuffix(string(fd.Message().Name()), "Order")

	desc := orderDescription{orderType: typeName}
	if fd.Kind() == protoreflect.MessageKind {
		innerMsg := msg.Get(fd).Message()
		desc.detail = extractScalarFields(innerMsg, verbose)
		desc.mapLines = extractStringMapEntries(innerMsg)
	}

	return desc
}

// extractScalarFields returns a compact "key=value" representation of all
// populated scalar fields in a protobuf message, recursing into nested messages.
// Lists, maps, and bytes are skipped. When verbose is false, multiline strings
// are also skipped.
func extractScalarFields(msg protoreflect.Message, verbose bool) string {
	var parts []string
	collectScalarFields(msg, verbose, &parts)

	return strings.Join(parts, " ")
}

// extractStringMapEntries recursively collects all string-to-string map entries
// from a protobuf message, returning them as key/value pairs.
func extractStringMapEntries(msg protoreflect.Message) [][2]string {
	var entries [][2]string
	collectStringMapEntries(msg, &entries)

	return entries
}

func collectScalarFields(msg protoreflect.Message, verbose bool, parts *[]string) {
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.IsList() || fd.IsMap() || fd.Kind() == protoreflect.BytesKind || fd.Kind() == protoreflect.GroupKind {
			return true
		}

		if fd.Kind() == protoreflect.MessageKind {
			collectScalarFields(v.Message(), verbose, parts)

			return true
		}

		s := fmt.Sprintf("%v", v.Interface())
		if fd.Kind() == protoreflect.EnumKind {
			s = string(fd.Enum().Values().ByNumber(v.Enum()).Name())
		}

		if !verbose && strings.Contains(s, "\n") {
			return true
		}

		*parts = append(*parts, fmt.Sprintf("%s=%s", fd.JSONName(), s))

		return true
	})
}

func collectStringMapEntries(msg protoreflect.Message, entries *[][2]string) {
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.IsMap() && fd.MapKey().Kind() == protoreflect.StringKind && fd.MapValue().Kind() == protoreflect.StringKind {
			v.Map().Range(func(mk protoreflect.MapKey, mv protoreflect.Value) bool {
				*entries = append(*entries, [2]string{mk.String(), mv.String()})

				return true
			})

			return true
		}

		if fd.Kind() == protoreflect.MessageKind && !fd.IsList() && !fd.IsMap() {
			collectStringMapEntries(v.Message(), entries)
		}

		return true
	})
}
