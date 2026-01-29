package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

// newLedgersListCommand creates the ledgers list command.
func newLedgersListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all ledgers",
		Long:  "List all ledgers in the cluster via gRPC",
		RunE:  runLedgersList,
	}

	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runLedgersList(cmd *cobra.Command, _ []string) error {
	client, conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := getContext(cmd)
	defer cancel()

	ledgers, err := getAllLedgersInfo(ctx, client)
	if err != nil {
		return fmt.Errorf("failed to list ledgers: %w", err)
	}

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(ledgers)
	}

	// Sort ledgers by name for consistent output
	names := make([]string, 0, len(ledgers))
	for name := range ledgers {
		names = append(names, name)
	}
	sort.Strings(names)

	if len(names) == 0 {
		fmt.Println("No ledgers found.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(w, "ID\tNAME\tCREATED AT\n")
	_, _ = fmt.Fprintf(w, "--\t----\t----------\n")

	for _, name := range names {
		ledger := ledgers[name]
		createdAt := "N/A"
		if ledger.CreatedAt != nil {
			createdAt = ledger.CreatedAt.AsTime().Format(time.RFC3339)
		}
		_, _ = fmt.Fprintf(w, "%d\t%s\t%s\n", ledger.Id, ledger.Name, createdAt)
	}

	return w.Flush()
}
