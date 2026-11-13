package numscripts

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewExecuteCommand executes a Numscript, whether it creates a transaction or only updates metadata.
func NewExecuteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "execute",
		Short: "Execute a Numscript",
		Long: `Execute a Numscript from a file or the ledger's library.

The script may create a transaction or update account metadata without one.
Exactly one of --file and --name is required. --version selects a saved version.

Examples:
  ledgerctl numscripts execute --ledger my-ledger --file transfer.num --var "amount=USD/2 1000"
  ledgerctl numscripts execute --ledger my-ledger --name transfer --version 1.0.0 --var "amount=USD/2 1000"`,
		Args: cobra.NoArgs,
		RunE: runExecute,
	}
	cmdutil.AddLedgerFlag(cmd)
	cmd.Flags().String("file", "", "Path to a Numscript file")
	cmd.Flags().String("name", "", "Name of a saved Numscript")
	cmd.Flags().String("version", "latest", "Version of a saved Numscript")
	cmd.Flags().StringArray("var", nil, "Script variable in name=value format (repeatable)")
	cmd.Flags().StringToString("metadata", nil, "Transaction metadata key=value pairs")
	cmd.Flags().String("reference", "", "Transaction reference (requires a script that creates postings)")
	cmd.Flags().String("idempotency-key", "", "Batch idempotency key for safe retries")
	cmd.Flags().Bool("force", false, "Bypass balance checks")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runExecute(cmd *cobra.Command, _ []string) error {
	file, _ := cmd.Flags().GetString("file")
	name, _ := cmd.Flags().GetString("name")
	if (file == "") == (name == "") {
		return errors.New("exactly one of --file and --name is required")
	}
	if file != "" && cmd.Flags().Changed("version") {
		return errors.New("--version can only be used with --name")
	}
	vars := make(map[string]string)
	values, _ := cmd.Flags().GetStringArray("var")
	for _, value := range values {
		key, val, ok := strings.Cut(value, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			return fmt.Errorf("invalid variable %q: expected name=value", value)
		}
		vars[key] = strings.TrimSpace(val)
	}

	payload := &servicepb.ExecuteNumscriptPayload{}
	if file != "" {
		content, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("reading Numscript file %q: %w", file, err)
		}
		if len(strings.TrimSpace(string(content))) == 0 {
			return errors.New("numscript file is empty")
		}
		payload.Script = &commonpb.Script{Plain: string(content), Vars: vars}
	} else {
		version, _ := cmd.Flags().GetString("version")
		payload.ScriptReference = &servicepb.ScriptReference{Name: name, Version: version, Vars: vars}
	}
	metadata, _ := cmd.Flags().GetStringToString("metadata")
	payload.Metadata = commonpb.MetadataFromGoMap(metadata)
	payload.Reference, _ = cmd.Flags().GetString("reference")
	payload.Force, _ = cmd.Flags().GetBool("force")
	ledger, _ := cmd.Flags().GetString(cmdutil.LedgerFlagName)
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()
	request, err := cmdutil.BuildApplyRequest(cmd, &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
		Ledger: ledger,
		Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_ExecuteNumscript{ExecuteNumscript: payload}},
	}}})
	if err != nil {
		return err
	}
	resp, err := client.Apply(ctx, request)
	if err != nil {
		return cmdutil.FormatGRPCError("failed to execute Numscript", err)
	}
	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		return fmt.Errorf("response signature verification failed: %w", err)
	}
	if len(resp.GetLogs()) != 1 || resp.GetLogs()[0].GetPayload().GetApply() == nil {
		return errors.New("unexpected Numscript response")
	}
	result := resp.GetLogs()[0].GetPayload().GetApply().GetLog().GetData()
	if result == nil {
		return errors.New("missing Numscript result")
	}
	if created := result.GetCreatedTransaction(); created != nil {
		if handled, err := cmdutil.EncodeStructured(cmd, created); handled || err != nil {
			return err
		}
		pterm.Printf("Created transaction #%d\n", created.GetTransaction().GetId())

		return nil
	}
	if updated := result.GetAccountMetadataUpdated(); updated != nil {
		if handled, err := cmdutil.EncodeStructured(cmd, updated); handled || err != nil {
			return err
		}
		pterm.Printf("Updated account metadata for %d account(s)\n", len(updated.GetAccountMetadata()))

		return nil
	}

	return errors.New("unexpected Numscript result type")
}
