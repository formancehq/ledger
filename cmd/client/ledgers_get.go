package main

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/spf13/cobra"
)

// newLedgersGetCommand creates the ledgers get command.
func newLedgersGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Get a ledger by name",
		Long:  "Get detailed information about a ledger by its name via gRPC",
		Args:  cobra.ExactArgs(1),
		RunE:  runLedgersGet,
	}

	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runLedgersGet(cmd *cobra.Command, args []string) error {
	ledgerName := args[0]

	client, conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := getContext(cmd)
	defer cancel()

	ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{
		Ledger: &servicepb.LedgerNameOrId{Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName}},
	})
	if err != nil {
		return fmt.Errorf("failed to get ledger: %w", err)
	}

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(ledger)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(w, "ID:\t%d\n", ledger.Id)
	_, _ = fmt.Fprintf(w, "Name:\t%s\n", ledger.Name)

	if ledger.CreatedAt != nil {
		_, _ = fmt.Fprintf(w, "Created At:\t%s\n", ledger.CreatedAt.AsTime().Format(time.RFC3339))
	}

	if len(ledger.Metadata) > 0 {
		_, _ = fmt.Fprintf(w, "\nMetadata:\n")
		for key, value := range ledger.Metadata {
			_, _ = fmt.Fprintf(w, "  %s:\t%s\n", key, value)
		}
	}

	return w.Flush()
}
