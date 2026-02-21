package ledgers

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewCreateCommand creates the ledgers create command.
func NewCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "create",
		Aliases: []string{"new", "add"},
		Short:   "Create a new ledger",
		Long:    "Create a new ledger via gRPC",
		Args:    cobra.NoArgs,
		RunE:    runCreate,
	}

	cmd.Flags().String("name", "", "Name of the ledger to create")
	cmd.Flags().StringToString("metadata", nil, "Metadata key=value pairs (can be repeated)")
	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runCreate(cmd *cobra.Command, _ []string) error {
	name, _ := cmd.Flags().GetString("name")

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

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Creating ledger %s...", name))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{
					Name:     name,
					Metadata: commonpb.MetadataSetFromMap(metadata),
				},
			},
		},
	}

	if err := cmdutil.SignRequests(cmd, requests); err != nil {
		spinner.Fail("Failed to sign request")
		return err
	}

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{Requests: requests})
	if err != nil {
		spinner.Fail("Failed to create ledger")
		return cmdutil.FormatGRPCError("failed to create ledger", err)
	}

	if err := cmdutil.VerifyResponseSignatures(cmd, resp.Logs); err != nil {
		spinner.Fail("Response signature verification failed")
		return fmt.Errorf("response signature verification failed: %w", err)
	}

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
