package logs

import (
	"errors"
	"fmt"
	"io"

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
	cmd.Flags().Uint64("checkpoint-id", 0, "Read from a query checkpoint instead of the live store")
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

	var (
		after, _        = cmd.Flags().GetUint64("after")
		pageSize, _     = cmd.Flags().GetUint32("page-size")
		minLogSeq, _    = cmd.Flags().GetUint64("min-log-sequence")
		checkpointID, _ = cmd.Flags().GetUint64("checkpoint-id")
		expand, _       = cmd.Flags().GetBool("expand")
	)

	req := &servicepb.ListLogsRequest{
		PageSize:       pageSize,
		MinLogSequence: minLogSeq,
		CheckpointId:   checkpointID,
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
		printLog(log, expand)
	}

	pterm.Println()
	pterm.Info.Printfln("%d log(s) displayed", len(entries))

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
