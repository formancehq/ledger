package signing

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewListKeysCommand creates the signing list-keys command.
func NewListKeysCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list-keys",
		Aliases: []string{"ls", "list"},
		Short:   "List all registered signing keys",
		Long:    "List all registered signing keys and their parent relationships",
		RunE:    runListKeys,
	}

	cmd.Flags().Bool("json", false, "Output as JSON")

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

	stream, err := client.ListSigningKeys(ctx, &servicepb.ListSigningKeysRequest{})
	if err != nil {
		return fmt.Errorf("listing signing keys: %w", err)
	}

	var keys []*commonpb.SigningKey

	for {
		key, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return fmt.Errorf("receiving signing key: %w", err)
		}

		keys = append(keys, key)
	}

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")

		return encoder.Encode(keys)
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

	return nil
}
