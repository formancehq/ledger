package logs

import (
	"fmt"
	"io"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// NewListCommand creates the logs list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "list",
		Aliases:           cmdutil.ListAliases,
		Short:             "List system logs",
		Long:              "List system log entries via gRPC streaming",
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("ledger", "", "Ledger name (required)")
	cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{SupportsReverse: true})
	cmdutil.AddFilterFlags(cmd, cmdutil.FilterOptions{})
	cmdutil.AddConsistencyFlags(cmd)
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	cmd.Flags().Bool("expand", false, "Expand details within each log entry")

	return cmd
}

// RenderList writes system logs, preserving --expand and structured output.
// Page cursors are supplied by the caller and emitted separately from the payload.
func RenderList(cmd *cobra.Command, entries []*commonpb.Log, cursors cmdutil.PageCursors) error {
	expand, _ := cmd.Flags().GetBool("expand")

	if handled, err := cmdutil.EncodeStructured(cmd, entries); handled || err != nil {
		// Surface the resume cursor on stderr so --json/--yaml payloads stay
		// lossless on stdout while scripts can still pick up the resume hint.
		cmdutil.EmitCursorHints(cmd, cursors)

		return err
	}

	if len(entries) == 0 {
		pterm.Info.WithWriter(cmd.OutOrStdout()).Println("No logs found.")
		cmdutil.EmitCursorHints(cmd, cursors)

		return nil
	}

	for _, log := range entries {
		printLogWithWriter(log, expand, cmd.OutOrStdout())
	}

	pterm.Fprintln(cmd.OutOrStdout())
	pterm.Info.WithWriter(cmd.OutOrStdout()).Printfln("%d log(s) displayed", len(entries))

	cmdutil.EmitCursorHints(cmd, cursors)

	return nil
}

// printLog prints a single system log in a human-readable format.
func printLog(log *commonpb.Log, expand bool) {
	printLogWithWriter(log, expand, pterm.DefaultBasicText.Writer)
}

func printLogWithWriter(log *commonpb.Log, expand bool, writer io.Writer) {
	text := pterm.DefaultBasicText.WithWriter(writer)

	desc := describeLog(log, expand)

	if expand {
		text.Printf("  #%-6d %s\n",
			log.GetSequence(),
			pterm.Cyan(desc.Type),
		)

		allLines := make([][2]string, 0, len(desc.Fields)+len(desc.MapLines))
		allLines = append(allLines, desc.Fields...)
		allLines = append(allLines, desc.MapLines...)

		if len(allLines) > 0 {
			maxKeyLen := 0
			for _, kv := range allLines {
				if len(kv[0]) > maxKeyLen {
					maxKeyLen = len(kv[0])
				}
			}

			for j, kv := range allLines {
				bullet := "├─"
				if j == len(allLines)-1 {
					bullet = "└─"
				}

				text.Printf("    %s %s %s %s\n",
					pterm.Gray(bullet),
					pterm.Yellow(fmt.Sprintf("%-*s", maxKeyLen, kv[0])),
					pterm.Gray("="),
					kv[1],
				)
			}
		}
	} else {
		if desc.Detail != "" {
			text.Printf("  #%-6d %s %s\n",
				log.GetSequence(),
				pterm.Cyan(desc.Type),
				pterm.Gray(desc.Detail),
			)
		} else {
			text.Printf("  #%-6d %s\n",
				log.GetSequence(),
				pterm.Cyan(desc.Type),
			)
		}
	}
}

// describeLog uses protobuf reflection to describe the log payload type and fields.
func describeLog(log *commonpb.Log, expand bool) cmdutil.OneofDescription {
	payload := log.GetPayload()
	if payload == nil {
		return cmdutil.OneofDescription{Type: "Unknown"}
	}

	// For Apply, unwrap to show the inner ledger log type.
	if apply := payload.GetApply(); apply != nil {
		innerLog := apply.GetLog()
		if innerLog != nil && innerLog.GetData() != nil {
			desc := cmdutil.DescribeOneofField(innerLog.GetData().ProtoReflect(), "payload", "Log", expand)
			desc.PrependField("ledger", apply.GetLedgerName())

			return desc
		}

		return cmdutil.OneofDescription{
			Type:   "Apply",
			Detail: "ledger=" + apply.GetLedgerName(),
			Fields: [][2]string{{"ledger", apply.GetLedgerName()}},
		}
	}

	return cmdutil.DescribeOneofField(payload.ProtoReflect(), "type", "Log", expand)
}
