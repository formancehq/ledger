package ledgers

import (
	"context"
	"errors"
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// Explicit-name promotion requires write permission, but the optional
// progress display requires read permission. Keep the confirmation available
// to write-only callers; the FSM still checks catch-up before promotion.
func optionalPromotionProgress(ctx context.Context, name string, getLedger func(context.Context, *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error)) (*commonpb.MirrorSyncProgress, error) {
	ledger, err := getLedger(ctx, &servicepb.GetLedgerRequest{Ledger: name})
	if status.Code(err) == codes.PermissionDenied {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return ledger.GetMirrorSyncProgress(), nil
}

// NewPromoteCommand creates the ledgers promote command.
func NewPromoteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "promote [name]",
		Short: "Promote a mirror ledger to normal mode",
		Long: `Promote a mirror ledger to normal mode via gRPC.

This stops mirror replication and converts the ledger to a regular read-write ledger.

Examples:
  ledgerctl ledgers promote my-ledger
  ledgerctl ledgers promote  # Interactive mode`,
		Args:              cobra.MaximumNArgs(1),
		RunE:              runPromote,
		ValidArgsFunction: cmdutil.CompleteLedgerNameArg,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runPromote(cmd *cobra.Command, args []string) error {
	var name string
	if len(args) > 0 {
		name = args[0]
	}

	if name == "" {
		client, conn, err := cmdutil.GetClient(cmd)
		if err != nil {
			return err
		}

		defer func() { _ = conn.Close() }()

		selectedName, err := cmdutil.SelectLedger(cmd, client)
		if err != nil {
			return err
		}

		name = selectedName
	}

	yes, _ := cmd.Flags().GetBool("yes")
	if !yes {
		client, conn, err := cmdutil.GetClient(cmd)
		if err != nil {
			return err
		}
		ctx, cancel := cmdutil.GetContext(cmd)
		progress, getErr := optionalPromotionProgress(ctx, name, func(ctx context.Context, request *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
			return client.GetLedger(ctx, request)
		})
		cancel()
		_ = conn.Close()
		if getErr != nil {
			return cmdutil.FormatGRPCError("failed to read mirror progress", getErr)
		}
		pterm.Warning.Printfln("You are about to promote mirror ledger %s to normal mode", name)
		pterm.Warning.Println("This will stop mirror replication permanently.")
		if progress != nil {
			pterm.Warning.Printfln("Mirror cursor: %d, source head: %d, remaining: %d",
				progress.GetCursor(), progress.GetSourceLogCount(), progress.GetRemainingLogs())
			if progress.GetError() != nil {
				pterm.Warning.Println("Mirror synchronization currently reports an error.")
			}
		}

		confirmed, err := cmdutil.PromptConfirm(pterm.DefaultInteractiveConfirm.
			WithDefaultText(fmt.Sprintf("Promote ledger '%s'?", name)).
			WithDefaultValue(false))
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

	spinner := cmdutil.StartSpinner(fmt.Sprintf("Promoting ledger %s...", name))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_PromoteLedger{
				PromoteLedger: &servicepb.PromoteLedgerRequest{
					Ledger: name,
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
