package main

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/numscript"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// suggestFilePaths provides file path suggestions for autocompletion.
func suggestFilePaths(toComplete string) []string {
	if toComplete == "" {
		toComplete = "."
	}

	// Get the directory to search in
	dir := filepath.Dir(toComplete)
	base := filepath.Base(toComplete)

	// If the path ends with a separator, search in that directory
	if strings.HasSuffix(toComplete, string(filepath.Separator)) || toComplete == "." {
		dir = toComplete
		base = ""
	}

	// Read the directory
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	var suggestions []string
	for _, entry := range entries {
		name := entry.Name()
		// Skip hidden files unless explicitly searching for them
		if strings.HasPrefix(name, ".") && !strings.HasPrefix(base, ".") {
			continue
		}

		// Check if the entry matches the partial input
		if base == "" || strings.HasPrefix(strings.ToLower(name), strings.ToLower(base)) {
			fullPath := filepath.Join(dir, name)
			if entry.IsDir() {
				fullPath += string(filepath.Separator)
			}
			suggestions = append(suggestions, fullPath)
		}
	}

	return suggestions
}

// newTransactionsCreateCommand creates the transactions create command.
func newTransactionsCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "create",
		Aliases: []string{"new", "add"},
		Short:   "Create a new transaction",
		Long: `Create a new transaction via gRPC.

Postings can be provided via flag, or use a Numscript file.
Flag format: --posting "source,destination,amount,asset"

Examples:
  ledgerctl transactions create --ledger my-ledger --posting "world,bank,1000,USD"
  ledgerctl transactions create --ledger my-ledger --posting "world,bank,1000,USD" --posting "bank,user,500,USD"
  ledgerctl transactions create --ledger my-ledger --script transfer.num --var "amount=1000" --var "asset=USD"
  ledgerctl transactions create --ledger my-ledger  # Interactive mode`,
		Args: cobra.NoArgs,
		RunE: runTransactionsCreate,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().StringArray("posting", nil, "Posting in format: source,destination,amount,asset (can be repeated)")
	cmd.Flags().String("script", "", "Path to a Numscript file (mutually exclusive with --posting)")
	cmd.Flags().StringArray("var", nil, "Script variable in format: name=value (can be repeated, only with --script)")
	cmd.Flags().String("reference", "", "Transaction reference")
	cmd.Flags().StringToString("metadata", nil, "Metadata key=value pairs")
	cmd.Flags().Bool("force", false, "Bypass balance checks (allow accounts to go negative)")
	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runTransactionsCreate(cmd *cobra.Command, _ []string) error {
	client, conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	// Get ledger name (from flag or interactive selection)
	ledgerFlag, _ := cmd.Flags().GetString("ledger")
	ledgerName, err := selectLedger(cmd, client, ledgerFlag)
	if err != nil {
		return err
	}

	// Get flags
	postingStrs, _ := cmd.Flags().GetStringArray("posting")
	scriptFile, _ := cmd.Flags().GetString("script")
	varStrs, _ := cmd.Flags().GetStringArray("var")

	// Validate mutual exclusivity
	if scriptFile != "" && len(postingStrs) > 0 {
		pterm.Error.Println("--script and --posting are mutually exclusive")
		return fmt.Errorf("--script and --posting are mutually exclusive")
	}

	if scriptFile == "" && len(varStrs) > 0 {
		pterm.Error.Println("--var can only be used with --script")
		return fmt.Errorf("--var can only be used with --script")
	}

	var (
		postings []*commonpb.Posting
		script   *commonpb.Script
	)

	if scriptFile != "" {
		// Read Numscript file
		scriptContent, err := os.ReadFile(scriptFile)
		if err != nil {
			pterm.Error.Printfln("Failed to read script file %q", scriptFile)
			return fmt.Errorf("failed to read script file %q: %w", scriptFile, err)
		}

		pterm.Info.Printfln("Using Numscript from %s", pterm.Cyan(scriptFile))

		// Parse variables from flags
		vars := make(map[string]string)
		for _, v := range varStrs {
			parts := strings.SplitN(v, "=", 2)
			if len(parts) != 2 {
				pterm.Error.Printfln("Invalid variable format %q: expected name=value", v)
				return fmt.Errorf("invalid variable format %q: expected name=value", v)
			}
			vars[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}

		// Parse the script to get required variables
		parsed := numscript.Parse(string(scriptContent))

		// Check for parsing errors
		if errs := parsed.GetParsingErrors(); len(errs) > 0 {
			pterm.Error.Println("Script parsing errors:")
			for _, e := range errs {
				pterm.Error.Printfln("  - %s", e.Msg)
			}
			return fmt.Errorf("numscript parse error: %s", numscript.ParseErrorsToString(errs, parsed.GetSource()))
		}

		// Get required variables and prompt for missing ones
		neededVars := parsed.GetNeededVariables()
		if len(neededVars) > 0 {
			// Sort variable names for consistent ordering
			varNames := make([]string, 0, len(neededVars))
			for name := range neededVars {
				varNames = append(varNames, name)
			}
			sort.Strings(varNames)

			// Check for missing variables and prompt for them
			missingVars := make([]string, 0)
			for _, name := range varNames {
				if _, exists := vars[name]; !exists {
					missingVars = append(missingVars, name)
				}
			}

			if len(missingVars) > 0 {
				pterm.Println()
				pterm.DefaultSection.Println("Script Variables")
				pterm.Info.Printfln("The script requires %d variable(s)", len(neededVars))
				pterm.Println()

				for _, name := range missingVars {
					varType := neededVars[name]
					value, err := promptVariable(name, varType)
					if err != nil {
						return err
					}
					vars[name] = value
				}
			}
		}

		script = &commonpb.Script{
			Plain: string(scriptContent),
			Vars:  vars,
		}
	} else if len(postingStrs) > 0 {
		// Parse postings from flags
		for _, ps := range postingStrs {
			posting, err := parsePosting(ps)
			if err != nil {
				pterm.Error.Printfln("Invalid posting %q: %v", ps, err)
				return fmt.Errorf("invalid posting %q: %w", ps, err)
			}
			postings = append(postings, posting)
		}
	} else {
		// Interactive mode: ask user to choose between Numscript and simple postings
		options := []string{"Simple postings", "Numscript file"}
		selectedOption, err := pterm.DefaultInteractiveSelect.
			WithDefaultText("How do you want to create this transaction?").
			WithOptions(options).
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		if selectedOption == "Numscript file" {
			// Prompt for script file path with autocompletion
			var scriptPath string
			prompt := &survey.Input{
				Message: "Path to Numscript file:",
				Suggest: suggestFilePaths,
			}
			if err := survey.AskOne(prompt, &scriptPath); err != nil {
				return fmt.Errorf("failed to read input: %w", err)
			}

			scriptContent, err := os.ReadFile(scriptPath)
			if err != nil {
				pterm.Error.Printfln("Failed to read script file %q", scriptPath)
				return fmt.Errorf("failed to read script file %q: %w", scriptPath, err)
			}

			// Parse the script to get required variables
			parsed := numscript.Parse(string(scriptContent))

			// Check for parsing errors
			if errs := parsed.GetParsingErrors(); len(errs) > 0 {
				pterm.Error.Println("Script parsing errors:")
				for _, e := range errs {
					pterm.Error.Printfln("  - %s", e.Msg)
				}
				return fmt.Errorf("numscript parse error: %s", numscript.ParseErrorsToString(errs, parsed.GetSource()))
			}

			// Get required variables and prompt for all of them
			vars := make(map[string]string)
			neededVars := parsed.GetNeededVariables()
			if len(neededVars) > 0 {
				varNames := make([]string, 0, len(neededVars))
				for name := range neededVars {
					varNames = append(varNames, name)
				}
				sort.Strings(varNames)

				pterm.Println()
				pterm.Println("Script variables:")

				for _, name := range varNames {
					varType := neededVars[name]
					value, err := promptVariable(name, varType)
					if err != nil {
						return err
					}
					vars[name] = value
				}
			}

			script = &commonpb.Script{
				Plain: string(scriptContent),
				Vars:  vars,
			}
		} else {
			// Interactive posting creation
			pterm.Println()
			pterm.Println("Create postings (at least one required):")
			pterm.Println()

			for {
				posting, err := promptPosting(len(postings) + 1)
				if err != nil {
					return err
				}
				postings = append(postings, posting)

				addAnother, err := pterm.DefaultInteractiveConfirm.
					WithDefaultText("Add another posting?").
					WithDefaultValue(false).
					Show()
				if err != nil {
					return fmt.Errorf("failed to read input: %w", err)
				}
				if !addAnother {
					break
				}
				pterm.Println()
			}
		}
	}

	// Validate that we have either postings or script
	if len(postings) == 0 && script == nil {
		pterm.Error.Println("Either postings or a script is required")
		return fmt.Errorf("either postings or a script is required")
	}

	// Get reference (optional)
	reference, _ := cmd.Flags().GetString("reference")

	// Get metadata (optional)
	metadata, _ := cmd.Flags().GetStringToString("metadata")

	// Get force flag
	force, _ := cmd.Flags().GetBool("force")

	// Create the transaction
	ctx, cancel := getContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Creating transaction...")

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{
			{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledgerName,
						Data: &servicepb.LedgerApplyRequest_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings:  postings,
								Script:    script,
								Reference: reference,
								Metadata:  commonpb.MetadataSetFromMap(metadata),
								Force:     force,
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		spinner.Fail("Failed to create transaction")
		return formatGRPCError("failed to create transaction", err)
	}

	// Extract the created transaction from the response
	if len(resp.Logs) == 0 {
		spinner.Fail("No response received")
		return fmt.Errorf("no response received")
	}

	log := resp.Logs[0]
	applyLog := log.Payload.GetApply()
	if applyLog == nil {
		spinner.Fail("Unexpected response type")
		return fmt.Errorf("unexpected response type")
	}

	createdTx := applyLog.Log.Data.GetCreatedTransaction()
	if createdTx == nil {
		spinner.Fail("Unexpected log payload type")
		return fmt.Errorf("unexpected log payload type")
	}

	tx := createdTx.Transaction

	spinner.Success("Created")

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(tx)
	}

	pterm.Println()

	// Display transaction header
	pterm.Printf("Transaction #%d\n", tx.Id)
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	// Display basic info
	if tx.Reference != "" {
		pterm.Printf("Reference:   %s\n", tx.Reference)
	}
	if tx.Timestamp != nil {
		pterm.Printf("Timestamp:   %s\n", pterm.Gray(tx.Timestamp.AsTime().Format("2006-01-02T15:04:05Z07:00")))
	}

	// Display postings
	if len(tx.Postings) > 0 {
		pterm.Println()
		pterm.Println("Postings:")

		postingsTable := pterm.TableData{
			{"#", "SOURCE", "", "DESTINATION", "AMOUNT", "ASSET"},
		}

		for i, posting := range tx.Postings {
			postingsTable = append(postingsTable, []string{
				fmt.Sprintf("%d", i+1),
				posting.Source,
				"→",
				posting.Destination,
				posting.Amount.Dec(),
				posting.Asset,
			})
		}

		if err := pterm.DefaultTable.WithHasHeader().WithData(postingsTable).Render(); err != nil {
			return err
		}
	}

	// Display metadata
	if tx.Metadata != nil && len(tx.Metadata.Metadata) > 0 {
		pterm.Println()
		pterm.Println("Metadata:")

		metadataTable := pterm.TableData{
			{"KEY", "VALUE"},
		}
		for _, md := range tx.Metadata.Metadata {
			metadataTable = append(metadataTable, []string{
				md.Key,
				md.Value.Value,
			})
		}
		return pterm.DefaultTable.WithHasHeader().WithData(metadataTable).Render()
	}

	return nil
}

// parsePosting parses a posting from string format "source,destination,amount,asset"
func parsePosting(s string) (*commonpb.Posting, error) {
	parts := strings.Split(s, ",")
	if len(parts) != 4 {
		return nil, fmt.Errorf("expected format: source,destination,amount,asset")
	}

	source := strings.TrimSpace(parts[0])
	destination := strings.TrimSpace(parts[1])
	amountStr := strings.TrimSpace(parts[2])
	asset := strings.TrimSpace(parts[3])

	if source == "" || destination == "" || amountStr == "" || asset == "" {
		return nil, fmt.Errorf("all fields are required")
	}

	amount, ok := new(big.Int).SetString(amountStr, 10)
	if !ok {
		return nil, fmt.Errorf("invalid amount: %s", amountStr)
	}

	return commonpb.NewPosting(source, destination, asset, amount), nil
}

// promptVariable prompts the user for a Numscript variable value based on its type.
func promptVariable(name, varType string) (string, error) {
	// Build prompt text with type hint
	var (
		promptText string
		hint       string
	)

	switch varType {
	case "account":
		promptText = fmt.Sprintf("Variable %s (%s)", pterm.Cyan("$"+name), pterm.Yellow("account"))
		hint = "e.g., users:alice, merchants:shop"
	case "monetary":
		promptText = fmt.Sprintf("Variable %s (%s)", pterm.Cyan("$"+name), pterm.Yellow("monetary"))
		hint = "e.g., USD/2 1000, EUR/2 50"
	case "string":
		promptText = fmt.Sprintf("Variable %s (%s)", pterm.Cyan("$"+name), pterm.Yellow("string"))
		hint = "e.g., order-123, ref-abc"
	case "number":
		promptText = fmt.Sprintf("Variable %s (%s)", pterm.Cyan("$"+name), pterm.Yellow("number"))
		hint = "e.g., 42, 100"
	case "portion":
		promptText = fmt.Sprintf("Variable %s (%s)", pterm.Cyan("$"+name), pterm.Yellow("portion"))
		hint = "e.g., 1/4, 25%, 0.25"
	default:
		promptText = fmt.Sprintf("Variable %s (%s)", pterm.Cyan("$"+name), pterm.Yellow(varType))
		hint = ""
	}

	if hint != "" {
		pterm.FgGray.Printfln("  %s", hint)
	}

	value, err := pterm.DefaultInteractiveTextInput.
		WithDefaultText(promptText).
		Show()
	if err != nil {
		return "", fmt.Errorf("failed to read variable %s: %w", name, err)
	}

	value = strings.TrimSpace(value)
	if value == "" {
		pterm.Error.Printfln("Variable $%s is required", name)
		return "", fmt.Errorf("variable $%s is required", name)
	}

	return value, nil
}

// promptPosting prompts the user to enter a posting interactively using pterm
func promptPosting(index int) (*commonpb.Posting, error) {
	pterm.FgLightCyan.Printfln("Posting #%d", index)

	// Source
	source, err := pterm.DefaultInteractiveTextInput.
		WithDefaultText("Source account").
		Show()
	if err != nil {
		return nil, fmt.Errorf("failed to read source: %w", err)
	}
	if source == "" {
		pterm.Error.Println("Source is required")
		return nil, fmt.Errorf("source is required")
	}

	// Destination
	destination, err := pterm.DefaultInteractiveTextInput.
		WithDefaultText("Destination account").
		Show()
	if err != nil {
		return nil, fmt.Errorf("failed to read destination: %w", err)
	}
	if destination == "" {
		pterm.Error.Println("Destination is required")
		return nil, fmt.Errorf("destination is required")
	}

	// Amount
	amountStr, err := pterm.DefaultInteractiveTextInput.
		WithDefaultText("Amount").
		Show()
	if err != nil {
		return nil, fmt.Errorf("failed to read amount: %w", err)
	}
	amount, ok := new(big.Int).SetString(amountStr, 10)
	if !ok || amount.Sign() <= 0 {
		pterm.Error.Println("Invalid amount: must be a positive integer")
		return nil, fmt.Errorf("invalid amount: must be a positive integer")
	}

	// Asset
	asset, err := pterm.DefaultInteractiveTextInput.
		WithDefaultText("Asset (e.g., USD, EUR)").
		Show()
	if err != nil {
		return nil, fmt.Errorf("failed to read asset: %w", err)
	}
	if asset == "" {
		pterm.Error.Println("Asset is required")
		return nil, fmt.Errorf("asset is required")
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
