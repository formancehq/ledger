package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newLedgersCreateCommand creates the ledgers create command.
func newLedgersCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "create",
		Aliases: []string{"new", "add"},
		Short:   "Create a new ledger",
		Long:    "Create a new ledger via gRPC",
		Args:    cobra.NoArgs,
		RunE:    runLedgersCreate,
	}

	cmd.Flags().String("name", "", "Name of the ledger to create")
	cmd.Flags().StringToString("metadata", nil, "Metadata key=value pairs (can be repeated)")
	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runLedgersCreate(cmd *cobra.Command, _ []string) error {
	name, _ := cmd.Flags().GetString("name")

	// If name is not provided via flag, prompt the user
	if name == "" {
		result, err := pterm.DefaultInteractiveTextInput.
			WithDefaultText("Enter ledger name").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}
		name = result
		if name == "" {
			pterm.Error.Println("Ledger name is required")
			return fmt.Errorf("ledger name is required")
		}
	}

	metadata, _ := cmd.Flags().GetStringToString("metadata")

	client, conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := getContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Creating ledger %s...", name))

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{
			{
				Type: &servicepb.Request_CreateLedger{
					CreateLedger: &servicepb.CreateLedgerRequest{
						Name:     name,
						Metadata: commonpb.MetadataSetFromMap(metadata),
					},
				},
			},
		},
	})
	if err != nil {
		spinner.Fail("Failed to create ledger")
		return formatGRPCError("failed to create ledger", err)
	}

	// Extract the created ledger info from the response
	if len(resp.Logs) == 0 {
		spinner.Fail("No response received")
		return fmt.Errorf("no response received")
	}

	log := resp.Logs[0]
	createLedgerLog := log.Payload.GetCreateLedger()
	if createLedgerLog == nil {
		spinner.Fail("Unexpected response type")
		return fmt.Errorf("unexpected response type")
	}

	ledger := createLedgerLog.Info

	spinner.Success("Created")

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(ledger)
	}

	pterm.Println()

	// Display ledger details
	pterm.Printf("Ledger: %s\n", ledger.Name)
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	pterm.Printf("ID:         %d\n", ledger.Id)
	pterm.Printf("Name:       %s\n", ledger.Name)
	createdAt := "-"
	if ledger.CreatedAt != nil {
		createdAt = ledger.CreatedAt.AsTime().Format(time.RFC3339)
	}
	pterm.Printf("Created At: %s\n", createdAt)

	return nil
}
