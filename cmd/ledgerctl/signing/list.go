package signing

import (
	"encoding/hex"
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewListKeysCommand creates the signing list-keys command.
func NewListKeysCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list-keys",
		Aliases: cmdutil.ListAliases,
		Short:   "List all registered signing keys",
		Long:    "List all registered signing keys and their parent relationships",
		RunE:    runListKeys,
	}

	cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{
		SupportsReverse: true,
	})
	cmdutil.AddMinLogSequenceFlag(cmd)
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runListKeys(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	pgn := cmdutil.GetPaginationFlags(cmd)
	minLogSeq, _ := cmd.Flags().GetUint64("min-log-sequence")

	keys, nextCursor, err := cmdutil.FetchSinglePageOrAll(cmd, pgn.Cursor, func(cur string) ([]*commonpb.SigningKey, metadata.MD, error) {
		page := pgn
		page.Cursor = cur

		stream, err := client.ListSigningKeys(ctx, &servicepb.ListSigningKeysRequest{
			Options: cmdutil.BuildListOptions(page, cmdutil.ConsistencyFlags{MinLogSequence: minLogSeq}, nil),
		})
		if err != nil {
			return nil, nil, fmt.Errorf("listing signing keys: %w", err)
		}

		items, recvErr := cmdutil.CollectStream(stream)
		if recvErr != nil {
			return nil, nil, fmt.Errorf("receiving signing keys: %w", recvErr)
		}

		return items, stream.Trailer(), nil
	})
	if err != nil {
		return err
	}

	if handled, err := cmdutil.EncodeStructured(cmd, keys); handled || err != nil {
		cmdutil.EmitNextCursorHint(cmd, nextCursor)

		return err
	}

	if len(keys) == 0 {
		pterm.Info.Println("No signing keys registered.")

		return nil
	}

	tableData := pterm.TableData{
		{"KEY ID", "PUBLIC KEY (HEX)", "PARENT"},
	}

	for _, k := range keys {
		parent := "(root)"
		if k.GetParentKeyId() != "" {
			parent = k.GetParentKeyId()
		}

		tableData = append(tableData, []string{
			k.GetKeyId(),
			hex.EncodeToString(k.GetPublicKey()),
			parent,
		})
	}

	if err := pterm.DefaultTable.WithHasHeader().WithData(tableData).Render(); err != nil {
		return fmt.Errorf("rendering table: %w", err)
	}

	pterm.Println()

	cmdutil.EmitNextCursorHint(cmd, nextCursor)

	return nil
}
