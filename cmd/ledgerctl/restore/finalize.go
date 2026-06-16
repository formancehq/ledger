package restore

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

// NewFinalizeCommand creates the restore finalize command.
func NewFinalizeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "finalize",
		Short:             "Finalize the restore and shut down the server",
		Long:              "Commit the staged backup as live data, write the RESTORED marker, and shut down the server",
		RunE:              runFinalize,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runFinalize(cmd *cobra.Command, _ []string) error {
	yes, _ := cmd.Flags().GetBool("yes")

	client, conn, err := getRestoreClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	// Show preview first
	resp, err := client.PreviewRestore(ctx, &restorepb.PreviewRestoreRequest{})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to preview restore", err)
	}

	printRestorePreview(resp)
	pterm.Println()

	// Confirm unless --yes
	if !yes {
		pterm.Warning.Println("This will finalize the restore and shut down the server.")
		pterm.Print("Continue? [y/N] ")

		reader := bufio.NewReader(os.Stdin)

		answer, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("reading confirmation: %w", err)
		}

		if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(answer)), "y") {
			pterm.Info.Println("Restore cancelled")

			return nil
		}
	}

	// Finalize
	finalizeResp, err := client.FinalizeRestore(ctx, &restorepb.FinalizeRestoreRequest{})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to finalize restore", err)
	}

	pterm.Success.Println(finalizeResp.GetMessage())

	return nil
}
