package logs

import (
	"errors"
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewListCommand creates the logs list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: cmdutil.ListAliases,
		Short:   "List system logs",
		Long:    "List system log entries via gRPC streaming",
		RunE:    runList,
	}

	cmd.Flags().String("ledger", "", "Ledger name (required)")
	cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{})
	cmdutil.AddFilterFlags(cmd, cmdutil.FilterOptions{})
	cmdutil.AddConsistencyFlags(cmd)
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	cmd.Flags().Bool("expand", false, "Expand details within each log entry")

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

	ledger, _ := cmd.Flags().GetString("ledger")
	expand, _ := cmd.Flags().GetBool("expand")
	pgn := cmdutil.GetPaginationFlags(cmd)
	flt := cmdutil.GetFilterFlags(cmd)
	cns := cmdutil.GetConsistencyFlags(cmd)

	if ledger == "" {
		return errors.New("--ledger flag is required")
	}

	filter, err := cmdutil.BuildQueryFilter(flt.Expr, flt.Prefix)
	if err != nil {
		return err
	}

	stream, err := client.ListLogs(ctx, &servicepb.ListLogsRequest{
		Ledger:  ledger,
		Options: cmdutil.BuildListOptions(pgn, cns, filter),
	})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to list logs", err)
	}

	// Drain to EOF so the x-next-cursor trailer is available; the server
	// already caps the stream at pageSize.
	entries, err := cmdutil.CollectStream(stream)
	if err != nil {
		return cmdutil.FormatGRPCError("receiving log", err)
	}

	nextCursor := cmdutil.NextCursorFromTrailer(stream.Trailer())

	if handled, err := cmdutil.EncodeStructured(cmd, entries); handled || err != nil {
		// Surface the resume cursor on stderr so --json/--yaml payloads stay
		// lossless on stdout while scripts can still pick up the resume hint.
		cmdutil.EmitNextCursorHint(cmd, nextCursor)

		return err
	}

	if len(entries) == 0 {
		pterm.Info.Println("No logs found.")

		return nil
	}

	for _, log := range entries {
		printLog(log, expand)
	}

	pterm.Println()
	pterm.Info.Printfln("%d log(s) displayed", len(entries))

	cmdutil.EmitNextCursorHint(cmd, nextCursor)

	return nil
}

// printLog prints a single system log in a human-readable format.
func printLog(log *commonpb.Log, expand bool) {
	desc := describeLog(log, expand)

	if expand {
		pterm.Printf("  #%-6d %s\n",
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

				pterm.Printf("    %s %s %s %s\n",
					pterm.Gray(bullet),
					pterm.Yellow(fmt.Sprintf("%-*s", maxKeyLen, kv[0])),
					pterm.Gray("="),
					kv[1],
				)
			}
		}
	} else {
		if desc.Detail != "" {
			pterm.Printf("  #%-6d %s %s\n",
				log.GetSequence(),
				pterm.Cyan(desc.Type),
				pterm.Gray(desc.Detail),
			)
		} else {
			pterm.Printf("  #%-6d %s\n",
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
