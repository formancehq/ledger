package transactions

import (
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
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

// NewCreateCommand creates the transactions create command.
func NewCreateCommand() *cobra.Command {
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
  ledgerctl transactions create --ledger my-ledger --posting "world,bank,1000,USD" --count 100
  ledgerctl transactions create --ledger my-ledger --posting "world,bank,1000,USD" --count 100 --batch 10
  ledgerctl transactions create --ledger my-ledger  # Interactive mode`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runCreate,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().StringArray("posting", nil, "Posting in format: source,destination,amount,asset (can be repeated)")
	cmd.Flags().String("script", "", "Path to a Numscript file (mutually exclusive with --posting)")
	cmd.Flags().StringArray("var", nil, "Script variable in format: name=value (can be repeated, only with --script)")
	cmd.Flags().String("reference", "", "Transaction reference")
	cmd.Flags().StringToString("metadata", nil, "Metadata key=value pairs")
	cmd.Flags().Bool("force", false, "Bypass balance checks (allow accounts to go negative)")
	cmd.Flags().Bool("expand-volumes", false, "Include post-commit volumes in response")
	cmd.Flags().Int("count", 1, "Number of times to send the transaction")
	cmd.Flags().Int("batch", 1, "Bundle the transactions into batches of this size (transactions per Apply request)")
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

	// Get ledger name (from flag or interactive selection)
	ledgerFlag, _ := cmd.Flags().GetString("ledger")

	ledgerName, err := cmdutil.SelectLedger(cmd, client, ledgerFlag)
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

		return cmdutil.Displayed(errors.New("--script and --posting are mutually exclusive"))
	}

	if scriptFile == "" && len(varStrs) > 0 {
		pterm.Error.Println("--var can only be used with --script")

		return cmdutil.Displayed(errors.New("--var can only be used with --script"))
	}

	var (
		postings []*commonpb.Posting
		script   *commonpb.Script
	)

	switch {
	case scriptFile != "":
		// Read Numscript file
		scriptContent, err := os.ReadFile(scriptFile)
		if err != nil {
			pterm.Error.Printfln("Failed to read script file %q", scriptFile)

			return cmdutil.Displayed(fmt.Errorf("failed to read script file %q: %w", scriptFile, err))
		}

		pterm.Info.Printfln("Using Numscript from %s", pterm.Cyan(scriptFile))

		// Parse variables from flags
		vars := make(map[string]string)

		for _, v := range varStrs {
			parts := strings.SplitN(v, "=", 2)
			if len(parts) != 2 {
				pterm.Error.Printfln("Invalid variable format %q: expected name=value", v)

				return cmdutil.Displayed(fmt.Errorf("invalid variable format %q: expected name=value", v))
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

			return cmdutil.Displayed(fmt.Errorf("numscript parse error: %s", numscript.ParseErrorsToString(errs, parsed.GetSource())))
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
	case len(postingStrs) > 0:
		// Parse postings from flags
		for _, ps := range postingStrs {
			posting, err := parsePosting(ps)
			if err != nil {
				pterm.Error.Printfln("Invalid posting %q: %v", ps, err)

				return cmdutil.Displayed(fmt.Errorf("invalid posting %q: %w", ps, err))
			}

			postings = append(postings, posting)
		}
	default:
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

				return cmdutil.Displayed(fmt.Errorf("failed to read script file %q: %w", scriptPath, err))
			}

			// Parse the script to get required variables
			parsed := numscript.Parse(string(scriptContent))

			// Check for parsing errors
			if errs := parsed.GetParsingErrors(); len(errs) > 0 {
				pterm.Error.Println("Script parsing errors:")

				for _, e := range errs {
					pterm.Error.Printfln("  - %s", e.Msg)
				}

				return cmdutil.Displayed(fmt.Errorf("numscript parse error: %s", numscript.ParseErrorsToString(errs, parsed.GetSource())))
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

		return cmdutil.Displayed(errors.New("either postings or a script is required"))
	}

	// Get reference (optional)
	reference, _ := cmd.Flags().GetString("reference")

	// Get metadata (optional)
	metadata, _ := cmd.Flags().GetStringToString("metadata")

	// Get force and expand-volumes flags
	force, _ := cmd.Flags().GetBool("force")
	expandVolumes, _ := cmd.Flags().GetBool("expand-volumes")

	// Get repetition and batching flags
	count, _ := cmd.Flags().GetInt("count")
	batchSize, _ := cmd.Flags().GetInt("batch")

	if count < 1 {
		pterm.Error.Println("--count must be at least 1")

		return cmdutil.Displayed(errors.New("--count must be at least 1"))
	}

	if batchSize < 1 {
		pterm.Error.Println("--batch must be at least 1")

		return cmdutil.Displayed(errors.New("--batch must be at least 1"))
	}

	// A transaction reference must be unique, so it cannot be reused across a
	// bulk create: the first transaction would commit and every subsequent one
	// would fail with a reference conflict, leaving a partial result.
	if reference != "" && count > 1 {
		pterm.Error.Println("--reference cannot be combined with --count > 1 (references must be unique)")

		return cmdutil.Displayed(errors.New("--reference cannot be combined with --count > 1 (references must be unique)"))
	}

	// newRequest builds a fresh Apply request for one transaction. It is called
	// once per transaction so each envelope is signed independently.
	newRequest := func() *servicepb.Request {
		return &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledgerName,
					Action: &servicepb.LedgerAction{
						Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings:      postings,
								Script:        script,
								Reference:     reference,
								Metadata:      commonpb.MetadataFromGoMap(metadata),
								Force:         force,
								ExpandVolumes: expandVolumes,
							},
						},
					},
				},
			},
		}
	}

	// applyBatch sends one Apply request under its own context so the
	// --timeout applies per request rather than cumulatively across batches.
	applyBatch := func(applyReq *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
		ctx, cancel := cmdutil.GetContext(cmd)
		defer cancel()

		return client.Apply(ctx, applyReq)
	}

	spinnerText := "Creating transaction..."
	if count > 1 {
		spinnerText = fmt.Sprintf("Creating %d transactions...", count)
	}

	spinner, _ := pterm.DefaultSpinner.Start(spinnerText)

	// Send count transactions, bundling up to batchSize of them per Apply
	// request. Collect every returned log so callers can inspect each result.
	var createdTxs []*commonpb.CreatedTransaction

	for sent := 0; sent < count; sent += batchSize {
		n := batchSize
		if remaining := count - sent; n > remaining {
			n = remaining
		}

		requests := make([]*servicepb.Request, n)
		for i := range requests {
			requests[i] = newRequest()
		}

		applyReq, err := cmdutil.BuildApplyRequest(cmd, requests...)
		if err != nil {
			spinner.Fail("Failed to sign request")

			return cmdutil.Displayed(err)
		}

		resp, err := applyBatch(applyReq)
		if err != nil {
			_ = spinner.Stop()

			return cmdutil.FormatGRPCError("failed to create transaction", err)
		}

		// Verify response signatures if a verification key is configured
		if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
			spinner.Fail("Response signature verification failed")

			return cmdutil.Displayed(fmt.Errorf("response signature verification failed: %w", err))
		}

		for _, log := range resp.GetLogs() {
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

			createdTxs = append(createdTxs, createdTx)
		}

		if count > 1 {
			spinner.UpdateText(fmt.Sprintf("Creating %d transactions... %d/%d", count, len(createdTxs), count))
		}
	}

	// Extract the created transaction(s) from the response
	if len(createdTxs) == 0 {
		spinner.Fail("No response received")

		return cmdutil.Displayed(errors.New("no response received"))
	}

	// Bulk mode: report a summary rather than the full detail of every
	// transaction, which would be unreadable at scale.
	if count > 1 {
		spinner.Success(fmt.Sprintf("Created %d transactions", len(createdTxs)))

		if handled, err := cmdutil.EncodeStructured(cmd, createdTxs); handled || err != nil {
			return err
		}

		pterm.Println()
		pterm.Printf("Created %s transactions across %s batch(es) of up to %s.\n",
			pterm.Cyan(strconv.Itoa(len(createdTxs))),
			pterm.Cyan(strconv.Itoa((count+batchSize-1)/batchSize)),
			pterm.Cyan(strconv.Itoa(batchSize)),
		)

		return nil
	}

	createdTx := createdTxs[0]
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

	// Display postings
	if len(tx.GetPostings()) > 0 {
		pterm.Println()
		pterm.Println("Postings:")

		postingsTable := pterm.TableData{
			{"#", "SOURCE", "", "DESTINATION", "AMOUNT", "ASSET"},
		}

		for i, posting := range tx.GetPostings() {
			postingsTable = append(postingsTable, []string{
				strconv.Itoa(i + 1),
				posting.GetSource(),
				"→",
				posting.GetDestination(),
				posting.GetAmount().Dec(),
				posting.GetAsset(),
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

	// Display post-commit volumes
	if createdTx.GetPostCommitVolumes() != nil {
		err := renderPostCommitVolumes(createdTx.GetPostCommitVolumes())
		if err != nil {
			return err
		}
	}

	return nil
}

// parsePosting parses a posting from string format "source,destination,amount,asset".
func parsePosting(s string) (*commonpb.Posting, error) {
	parts := strings.Split(s, ",")
	if len(parts) != 4 {
		return nil, errors.New("expected format: source,destination,amount,asset")
	}

	source := strings.TrimSpace(parts[0])
	destination := strings.TrimSpace(parts[1])
	amountStr := strings.TrimSpace(parts[2])
	asset := strings.TrimSpace(parts[3])

	if source == "" || destination == "" || amountStr == "" || asset == "" {
		return nil, errors.New("all fields are required")
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

		return "", cmdutil.Displayed(fmt.Errorf("variable $%s is required", name))
	}

	return value, nil
}

// promptPosting prompts the user to enter a posting interactively using pterm.
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

		return nil, cmdutil.Displayed(errors.New("source is required"))
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

		return nil, cmdutil.Displayed(errors.New("destination is required"))
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

		return nil, cmdutil.Displayed(errors.New("invalid amount: must be a positive integer"))
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
