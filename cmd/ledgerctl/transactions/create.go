package transactions

import (
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewCreateCommand creates the transactions create command.
func NewCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "create",
		Aliases: []string{"new", "add"},
		Short:   "Create a new transaction",
		Long: `Create a new transaction via gRPC.

Postings can be provided via flags or interactively.
Flag format: --posting "source,destination,amount,asset[,color]"

Examples:
  ledgerctl transactions create --ledger my-ledger --posting "world,bank,1000,USD"
  ledgerctl transactions create --ledger my-ledger --posting "world,bank,1000,USD" --posting "bank,user,500,USD"
  ledgerctl transactions create --ledger my-ledger  # Interactive mode`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runCreate,
	}

	cmdutil.AddLedgerFlag(cmd)
	cmd.Flags().StringArray("posting", nil, "Posting in format: source,destination,amount,asset[,color] (can be repeated)")
	cmd.Flags().String("reference", "", "Transaction reference")
	cmd.Flags().StringToString("metadata", nil, "Metadata key=value pairs")
	cmd.Flags().Bool("force", false, "Bypass balance checks (allow accounts to go negative)")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runCreate(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ledgerName, _ := cmd.Flags().GetString(cmdutil.LedgerFlagName)

	// Get postings from flags or prompt for them interactively.
	postingStrs, _ := cmd.Flags().GetStringArray("posting")
	var postings []*commonpb.Posting
	if len(postingStrs) > 0 {
		for _, ps := range postingStrs {
			posting, err := parsePosting(ps)
			if err != nil {
				return fmt.Errorf("invalid posting %q: %w", ps, err)
			}
			postings = append(postings, posting)
		}
	} else {
		for {
			posting, err := promptPosting(len(postings) + 1)
			if err != nil {
				return err
			}
			postings = append(postings, posting)
			addAnother, err := cmdutil.PromptConfirm(pterm.DefaultInteractiveConfirm.
				WithDefaultText("Add another posting?").
				WithDefaultValue(false))
			if err != nil {
				return fmt.Errorf("failed to read input: %w", err)
			}
			if !addAnother {
				break
			}
		}
	}

	// Get reference (optional)
	reference, _ := cmd.Flags().GetString("reference")

	// Get metadata (optional)
	metadata, _ := cmd.Flags().GetStringToString("metadata")

	// Get force flag
	force, _ := cmd.Flags().GetBool("force")

	// Create the transaction
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner := cmdutil.StartSpinner("Creating transaction...")

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledgerName,
					Action: &servicepb.LedgerAction{
						Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings:  postings,
								Reference: reference,
								Metadata:  commonpb.MetadataFromGoMap(metadata),
								Force:     force,
							},
						},
					},
				},
			},
		},
	}

	applyReq, err := cmdutil.BuildApplyRequest(cmd, requests...)
	if err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	resp, err := client.Apply(ctx, applyReq)
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to create transaction", err)
	}

	// Verify response signatures if a verification key is configured
	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		spinner.Fail("Response signature verification failed")

		return cmdutil.Displayed(fmt.Errorf("response signature verification failed: %w", err))
	}

	// Extract the created transaction from the response
	if len(resp.GetLogs()) == 0 {
		spinner.Fail("No response received")

		return cmdutil.Displayed(errors.New("no response received"))
	}

	log := resp.GetLogs()[0]

	applyLog := log.GetPayload().GetApply()
	if applyLog == nil {
		spinner.Fail("Unexpected response type")

		return cmdutil.Displayed(errors.New("unexpected response type"))
	}

	createdTx := applyLog.GetLog().GetData().GetCreatedTransaction()
	if createdTx == nil {
		spinner.Fail("Unexpected log payload type")

		return cmdutil.Displayed(errors.New("unexpected log payload type"))
	}

	tx := createdTx.GetTransaction()

	spinner.Success("Created")

	if handled, err := cmdutil.EncodeStructured(cmd, createdTx); handled || err != nil {
		return err
	}

	pterm.Println()

	// Display transaction header
	pterm.Printf("Transaction: %s\n", pterm.Cyan(fmt.Sprintf("#%d", tx.GetId())))
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	// Display basic info
	if tx.GetReference() != "" {
		pterm.Printf("Reference:   %s\n", tx.GetReference())
	}

	if tx.GetTimestamp() != nil {
		pterm.Printf("Timestamp:   %s\n", pterm.Gray(tx.GetTimestamp().AsTime().Format("2006-01-02T15:04:05Z07:00")))
	}

	rescale := cmdutil.RescaleTarget(cmd)

	// Display postings
	if len(tx.GetPostings()) > 0 {
		pterm.Println()
		pterm.Println("Postings:")

		postingsTable := pterm.TableData{
			{"#", "SOURCE", "", "DESTINATION", "AMOUNT", "ASSET", "COLOR"},
		}

		for i, posting := range tx.GetPostings() {
			amount, asset := posting.GetAmount().Dec(), posting.GetAsset()
			if rescale != nil {
				var err error

				amount, asset, err = cmdutil.Rescale(amount, asset, *rescale)
				if err != nil {
					return err
				}
			}

			color := posting.GetColor()
			if color == "" {
				color = "-"
			}

			postingsTable = append(postingsTable, []string{
				strconv.Itoa(i + 1),
				posting.GetSource(),
				"→",
				posting.GetDestination(),
				amount,
				asset,
				color,
			})
		}

		err := pterm.DefaultTable.WithHasHeader().WithData(postingsTable).Render()
		if err != nil {
			return err
		}
	}

	// Display metadata
	if len(tx.GetMetadata()) > 0 {
		pterm.Println()
		pterm.Println("Metadata:")

		metadataTable := pterm.TableData{
			{"KEY", "VALUE"},
		}
		for key, value := range tx.GetMetadata() {
			metadataTable = append(metadataTable, []string{
				key,
				commonpb.MetadataValueToString(value),
			})
		}

		err := pterm.DefaultTable.WithHasHeader().WithData(metadataTable).Render()
		if err != nil {
			return err
		}
	}

	// Display post-commit volumes (carried on the created transaction)
	if pcv := createdTx.GetTransaction().GetPostCommitVolumes(); pcv != nil {
		err := renderPostCommitVolumes(pcv, rescale)
		if err != nil {
			return err
		}
	}

	return nil
}

// parsePosting parses a posting from string format
//   - "source,destination,amount,asset"            (uncolored, 4 fields)
//   - "source,destination,amount,asset,color"      (colored,   5 fields)
//
// The color field is optional. An empty fifth field is treated as no color.
func parsePosting(s string) (*commonpb.Posting, error) {
	parts := strings.Split(s, ",")
	if len(parts) != 4 && len(parts) != 5 {
		return nil, errors.New("expected format: source,destination,amount,asset[,color]")
	}

	source := strings.TrimSpace(parts[0])
	destination := strings.TrimSpace(parts[1])
	amountStr := strings.TrimSpace(parts[2])
	asset := strings.TrimSpace(parts[3])
	color := ""
	if len(parts) == 5 {
		color = strings.TrimSpace(parts[4])
	}

	if source == "" || destination == "" || amountStr == "" || asset == "" {
		return nil, errors.New("source, destination, amount and asset are required")
	}

	amount, ok := new(big.Int).SetString(amountStr, 10)
	if !ok {
		return nil, fmt.Errorf("invalid amount: %s", amountStr)
	}
	if amount.Sign() < 0 || amount.BitLen() > 256 {
		return nil, errors.New("amount must be between 0 and 2^256 - 1")
	}

	return commonpb.NewColoredPosting(source, destination, asset, color, amount), nil
}

// promptPosting prompts the user to enter a posting interactively using pterm.
func promptPosting(index int) (*commonpb.Posting, error) {
	pterm.FgLightCyan.Printfln("Posting #%d", index)

	// Source
	source, err := cmdutil.PromptText(pterm.DefaultInteractiveTextInput.
		WithDefaultText("Source account"))
	if err != nil {
		return nil, fmt.Errorf("failed to read source: %w", err)
	}

	if source == "" {
		pterm.Error.Println("Source is required")

		return nil, cmdutil.Displayed(errors.New("source is required"))
	}

	// Destination
	destination, err := cmdutil.PromptText(pterm.DefaultInteractiveTextInput.
		WithDefaultText("Destination account"))
	if err != nil {
		return nil, fmt.Errorf("failed to read destination: %w", err)
	}

	if destination == "" {
		pterm.Error.Println("Destination is required")

		return nil, cmdutil.Displayed(errors.New("destination is required"))
	}

	// Amount
	amountStr, err := cmdutil.PromptText(pterm.DefaultInteractiveTextInput.
		WithDefaultText("Amount"))
	if err != nil {
		return nil, fmt.Errorf("failed to read amount: %w", err)
	}

	amount, ok := new(big.Int).SetString(amountStr, 10)
	if !ok || amount.Sign() <= 0 {
		pterm.Error.Println("Invalid amount: must be a positive integer")

		return nil, cmdutil.Displayed(errors.New("invalid amount: must be a positive integer"))
	}
	if amount.BitLen() > 256 {
		pterm.Error.Println("Invalid amount: amount must not exceed 2^256 - 1")

		return nil, cmdutil.Displayed(errors.New("invalid amount: amount must not exceed 2^256 - 1"))
	}

	// Asset
	asset, err := cmdutil.PromptText(pterm.DefaultInteractiveTextInput.
		WithDefaultText("Asset (e.g., USD, EUR)"))
	if err != nil {
		return nil, fmt.Errorf("failed to read asset: %w", err)
	}

	if asset == "" {
		pterm.Error.Println("Asset is required")

		return nil, cmdutil.Displayed(errors.New("asset is required"))
	}

	// Show summary
	pterm.Success.Printfln("Posting: %s → %s (%s %s)",
		pterm.Red(source),
		pterm.Green(destination),
		amountStr,
		pterm.Yellow(asset),
	)

	return commonpb.NewPosting(source, destination, asset, amount), nil
}
