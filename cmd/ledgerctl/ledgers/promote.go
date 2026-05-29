package ledgers

import (
	"errors"
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewPromoteCommand creates the ledgers promote command.
func NewPromoteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "promote [name]",
		Short: "Promote a mirror ledger to normal mode",
		Long: `Promote a mirror ledger to normal mode via gRPC.

This stops mirror replication and converts the ledger to a regular read-write ledger.

Examples:
  ledgerctl ledgers promote my-ledger
  ledgerctl ledgers promote --name my-ledger
  ledgerctl ledgers promote  # Interactive mode`,
		Args: cobra.MaximumNArgs(1),
		RunE: runPromote,
	}

	cmd.Flags().String("name", "", "Name of the ledger to promote")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runPromote(cmd *cobra.Command, args []string) error {
	name, _ := cmd.Flags().GetString("name")
	if len(args) > 0 {
		name = args[0]
	}

	if name == "" {
		client, conn, err := cmdutil.GetClient(cmd)
		if err != nil {
			return err
		}

		defer func() { _ = conn.Close() }()

		selectedName, err := cmdutil.SelectLedger(cmd, client, "")
		if err != nil {
			return err
		}

		name = selectedName
	}

	yes, _ := cmd.Flags().GetBool("yes")
	if !yes {
		pterm.Warning.Printfln("You are about to promote mirror ledger %s to normal mode", name)
		pterm.Warning.Println("This will stop mirror replication permanently.")

		confirmed, err := pterm.DefaultInteractiveConfirm.
			WithDefaultText(fmt.Sprintf("Promote ledger '%s'?", name)).
			WithDefaultValue(false).
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		if !confirmed {
			pterm.Info.Println("Promotion cancelled.")

			return nil
		}
	}

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Promoting ledger %s...", name))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_PromoteLedger{
				PromoteLedger: &servicepb.PromoteLedgerRequest{
					Ledger: name,
				},
			},
		},
	}

	if err := cmdutil.SignRequests(cmd, requests); err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{Requests: requests})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to promote ledger", err)
	}

	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		spinner.Fail("Response signature verification failed")

		return cmdutil.Displayed(fmt.Errorf("response signature verification failed: %w", err))
	}

	if len(resp.GetLogs()) == 0 {
		spinner.Fail("No response received")

		return cmdutil.Displayed(errors.New("no response received"))
	}

	log := resp.GetLogs()[0]

	promoteLedgerLog := log.GetPayload().GetPromoteLedger()
	if promoteLedgerLog == nil {
		spinner.Fail("Unexpected response type")

		return cmdutil.Displayed(errors.New("unexpected response type"))
	}

	spinner.Success("Promoted")

	if handled, err := cmdutil.EncodeStructured(cmd, promoteLedgerLog); handled || err != nil {
		return err
	}

	pterm.Println()

	pterm.Printf("Ledger: %s (promoted to normal mode)\n", promoteLedgerLog.GetName())
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	pterm.Printf("Name:       %s\n", pterm.Gray(promoteLedgerLog.GetName()))
	pterm.Printf("Mode:       normal\n")

	return nil
}
