package transactions

import (
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"golang.org/x/term"

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
Flag format: --posting "source,destination,amount,asset[,color]"

Examples:
  ledgerctl transactions create --ledger my-ledger --posting "world,bank,1000,USD"
  ledgerctl transactions create --ledger my-ledger --posting "world,bank,1000,USD" --posting "bank,user,500,USD"
  ledgerctl transactions create --ledger my-ledger --script transfer.num --var "amount=1000" --var "asset=USD"
  ledgerctl transactions create --ledger my-ledger  # Interactive mode`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().StringArray("posting", nil, "Posting in format: source,destination,amount,asset[,color] (can be repeated)")
	cmd.Flags().String("script", "", "Path to a Numscript file (mutually exclusive with --posting)")
	cmd.Flags().StringArray("var", nil, "Script variable in format: name=value (can be repeated, only with --script)")
	cmd.Flags().String("reference", "", "Transaction reference")
	cmd.Flags().StringToString("metadata", nil, "Metadata key=value pairs")
	cmd.Flags().Bool("force", false, "Bypass balance checks (allow accounts to go negative)")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

// PrepareCreate acquires transaction input for the selected ledger without making RPCs.
func PrepareCreate(cmd *cobra.Command, ledgerName string) (*servicepb.CreateTransactionPayload, error) {
	ctx := cmdutil.CommandContextOrBackground(cmd)
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if ledgerName == "" {
		return nil, errors.New("ledger name is required (use --ledger flag)")
	}
	// Get flags
	postingStrs, err := cmd.Flags().GetStringArray("posting")
	if err != nil {
		return nil, err
	}
	scriptFile, err := cmd.Flags().GetString("script")
	if err != nil {
		return nil, err
	}
	varStrs, err := cmd.Flags().GetStringArray("var")
	if err != nil {
		return nil, err
	}

	// Validate mutual exclusivity
	if scriptFile != "" && len(postingStrs) > 0 {
		return nil, errors.New("--script and --posting are mutually exclusive")
	}

	if scriptFile == "" && len(varStrs) > 0 {
		return nil, errors.New("--var can only be used with --script")
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
			return nil, fmt.Errorf("failed to read script file %q: %w", scriptFile, err)
		}

		// Parse variables from flags
		vars := make(map[string]string)

		for _, v := range varStrs {
			parts := strings.SplitN(v, "=", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid variable format %q: expected name=value", v)
			}

			vars[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}

		// Parse the script to get required variables
		parsed := numscript.Parse(string(scriptContent))

		// Check for parsing errors
		if errs := parsed.GetParsingErrors(); len(errs) > 0 {
			return nil, fmt.Errorf("numscript parse error: %s", numscript.ParseErrorsToString(errs, parsed.GetSource()))
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
				if !createInputIsTerminal(cmd) {
					return nil, fmt.Errorf("missing script variables (use --var): %s", strings.Join(missingVars, ", "))
				}
				pterm.Fprintln(cmd.ErrOrStderr())
				pterm.DefaultSection.WithWriter(cmd.ErrOrStderr()).Println("Script Variables")
				pterm.Info.WithWriter(cmd.ErrOrStderr()).Printfln("The script requires %d variable(s)", len(neededVars))
				pterm.Fprintln(cmd.ErrOrStderr())

				for _, name := range missingVars {
					varType := neededVars[name]

					value, err := promptVariable(cmd, name, varType)
					if err != nil {
						return nil, err
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
				return nil, fmt.Errorf("invalid posting %q: %w", ps, err)
			}

			postings = append(postings, posting)
		}
	default:
		if !createInputIsTerminal(cmd) {
			return nil, errors.New("transaction input is required (use --posting, --script, or --data)")
		}
		// Interactive mode: ask user to choose between Numscript and simple postings
		options := []string{"Simple postings", "Numscript file"}

		var selectedOption string
		if err := askCreate(cmd, &survey.Select{Message: "How do you want to create this transaction?", Options: options}, &selectedOption); err != nil {
			return nil, fmt.Errorf("failed to read input: %w", err)
		}

		if selectedOption == "Numscript file" {
			// Prompt for script file path with autocompletion
			var scriptPath string

			prompt := &survey.Input{
				Message: "Path to Numscript file:",
				Suggest: suggestFilePaths,
			}
			if err := askCreate(cmd, prompt, &scriptPath); err != nil {
				return nil, fmt.Errorf("failed to read input: %w", err)
			}

			scriptContent, err := os.ReadFile(scriptPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read script file %q: %w", scriptPath, err)
			}

			// Parse the script to get required variables
			parsed := numscript.Parse(string(scriptContent))

			// Check for parsing errors
			if errs := parsed.GetParsingErrors(); len(errs) > 0 {
				return nil, fmt.Errorf("numscript parse error: %s", numscript.ParseErrorsToString(errs, parsed.GetSource()))
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

				pterm.Fprintln(cmd.ErrOrStderr())
				pterm.Fprintln(cmd.ErrOrStderr(), "Script variables:")

				for _, name := range varNames {
					varType := neededVars[name]

					value, err := promptVariable(cmd, name, varType)
					if err != nil {
						return nil, err
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
			pterm.Fprintln(cmd.ErrOrStderr())
			pterm.Fprintln(cmd.ErrOrStderr(), "Create postings (at least one required):")
			pterm.Fprintln(cmd.ErrOrStderr())

			for {
				posting, err := promptPosting(cmd, len(postings)+1)
				if err != nil {
					return nil, err
				}

				postings = append(postings, posting)

				var addAnother bool
				if err := askCreate(cmd, &survey.Confirm{Message: "Add another posting?", Default: false}, &addAnother); err != nil {
					return nil, fmt.Errorf("failed to read input: %w", err)
				}

				if !addAnother {
					break
				}

				pterm.Fprintln(cmd.ErrOrStderr())
			}
		}
	}

	// Validate that we have either postings or script
	if len(postings) == 0 && script == nil {
		return nil, errors.New("either postings or a script is required")
	}

	// Get reference (optional)
	reference, err := cmd.Flags().GetString("reference")
	if err != nil {
		return nil, err
	}

	// Get metadata (optional)
	metadata, err := cmd.Flags().GetStringToString("metadata")
	if err != nil {
		return nil, err
	}

	// Get force flag
	force, err := cmd.Flags().GetBool("force")
	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return &servicepb.CreateTransactionPayload{
		Postings:  postings,
		Script:    script,
		Reference: reference,
		Metadata:  commonpb.MetadataFromGoMap(metadata),
		Force:     force,
	}, nil
}

// RenderCreate validates and renders the native create response without starting a spinner.
func RenderCreate(cmd *cobra.Command, resp *servicepb.ApplyResponse) error {
	return renderCreate(cmd, resp, nil)
}

func renderCreate(cmd *cobra.Command, resp *servicepb.ApplyResponse, spinner *cmdutil.Spinner) error {
	if cmdutil.IsStructuredOutput(cmd) && spinner != nil {
		_ = spinner.Stop() // Structured output must not emit spinner prefixes.
		spinner = nil
	}

	fail := func(message string, err error) error {
		if spinner == nil {
			return err
		}
		spinner.Fail(message)

		return cmdutil.Displayed(err)
	}

	// Verify response signatures if a verification key is configured
	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		return fail("Response signature verification failed", fmt.Errorf("response signature verification failed: %w", err))
	}

	// Extract the created transaction from the response
	if len(resp.GetLogs()) == 0 {
		return fail("No response received", errors.New("no response received"))
	}

	log := resp.GetLogs()[0]

	applyLog := log.GetPayload().GetApply()
	if applyLog == nil {
		return fail("Unexpected response type", errors.New("unexpected response type"))
	}

	createdTx := applyLog.GetLog().GetData().GetCreatedTransaction()
	if createdTx == nil {
		return fail("Unexpected log payload type", errors.New("unexpected log payload type"))
	}

	tx := createdTx.GetTransaction()

	if handled, err := cmdutil.EncodeStructured(cmd, createdTx); handled || err != nil {
		return err
	}

	if spinner != nil {
		spinner.Success("Created")
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
		err := renderPostCommitVolumes(cmd.OutOrStdout(), pcv, rescale)
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

	return commonpb.NewColoredPosting(source, destination, asset, color, amount), nil
}

// promptVariable prompts the user for a Numscript variable value based on its type.
func promptVariable(cmd *cobra.Command, name, varType string) (string, error) {
	if err := cmdutil.CommandContextOrBackground(cmd).Err(); err != nil {
		return "", err
	}

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
		pterm.Fprintln(cmd.ErrOrStderr(), "  "+pterm.Gray(hint))
	}

	var value string
	if err := askCreate(cmd, &survey.Input{Message: promptText}, &value); err != nil {
		return "", fmt.Errorf("failed to read variable %s: %w", name, err)
	}

	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("variable $%s is required", name)
	}

	return value, nil
}

// promptPosting prompts the user to enter a posting interactively on stderr.
func promptPosting(cmd *cobra.Command, index int) (*commonpb.Posting, error) {
	if err := cmdutil.CommandContextOrBackground(cmd).Err(); err != nil {
		return nil, err
	}

	pterm.Fprintln(cmd.ErrOrStderr(), fmt.Sprintf("Posting #%d", index))

	// Source
	var source string
	if err := askCreate(cmd, &survey.Input{Message: "Source account"}, &source); err != nil {
		return nil, fmt.Errorf("failed to read source: %w", err)
	}

	if source == "" {
		return nil, errors.New("source is required")
	}

	// Destination
	var destination string
	if err := askCreate(cmd, &survey.Input{Message: "Destination account"}, &destination); err != nil {
		return nil, fmt.Errorf("failed to read destination: %w", err)
	}

	if destination == "" {
		return nil, errors.New("destination is required")
	}

	// Amount
	var amountStr string
	if err := askCreate(cmd, &survey.Input{Message: "Amount"}, &amountStr); err != nil {
		return nil, fmt.Errorf("failed to read amount: %w", err)
	}

	amount, ok := new(big.Int).SetString(amountStr, 10)
	if !ok || amount.Sign() <= 0 {
		return nil, errors.New("invalid amount: must be a positive integer")
	}

	// Asset
	var asset string
	if err := askCreate(cmd, &survey.Input{Message: "Asset (e.g., USD, EUR)"}, &asset); err != nil {
		return nil, fmt.Errorf("failed to read asset: %w", err)
	}

	if asset == "" {
		return nil, errors.New("asset is required")
	}

	// Show summary
	pterm.Fprintln(cmd.ErrOrStderr(), fmt.Sprintf("Posting: %s → %s (%s %s)",
		pterm.Red(source),
		pterm.Green(destination),
		amountStr,
		pterm.Yellow(asset),
	))

	return commonpb.NewPosting(source, destination, asset, amount), nil
}

func askCreate(cmd *cobra.Command, prompt survey.Prompt, response any) error {
	ctx := cmdutil.CommandContextOrBackground(cmd)
	if err := ctx.Err(); err != nil {
		return err
	}

	input, ok := cmd.InOrStdin().(*os.File)
	if !ok {
		return errors.New("interactive input requires a terminal")
	}

	return cmdutil.AskOneContext(ctx, prompt, response, input, createPromptOutput{Writer: cmd.ErrOrStderr()}, cmd.ErrOrStderr())
}

func createInputIsTerminal(cmd *cobra.Command) bool {
	input, ok := cmd.InOrStdin().(*os.File)

	return ok && term.IsTerminal(int(input.Fd()))
}

// createPromptOutput supplies Survey with stderr's terminal descriptor while
// sending prompt bytes through the command's scoped error writer.
type createPromptOutput struct{ io.Writer }

func (createPromptOutput) Fd() uintptr { return os.Stderr.Fd() }
