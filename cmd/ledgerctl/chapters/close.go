package chapters

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewCloseCommand creates the chapters close command.
func NewCloseCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "close",
		Short:             "Close the current open chapter",
		Long:              "Close the current open chapter and open a new one. A background seal process will compute the sealing hash.",
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runClose,
	}

	cmdutil.AddOutputFlags(cmd)

	return cmd
}

func runClose(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Envelopes: servicepb.UnsignedEnvelopes(&servicepb.Request{
			Type: &servicepb.Request_CloseChapter{
				CloseChapter: &servicepb.CloseChapterRequest{},
			},
		}),
	})
	if err != nil {
		return fmt.Errorf("closing chapter: %w", err)
	}

	if handled, err := cmdutil.EncodeStructured(cmd, resp); handled || err != nil {
		return err
	}

	if len(resp.GetLogs()) > 0 {
		log := resp.GetLogs()[0]
		if closeChapterLog := log.GetPayload().GetCloseChapter(); closeChapterLog != nil {
			pterm.Success.Printfln("Chapter %d closed successfully", closeChapterLog.GetClosedChapter().GetId())
			pterm.Info.Printfln("New chapter %d opened", closeChapterLog.GetNewChapter().GetId())
			pterm.Info.Println("Background sealing process will compute the sealing hash")
		} else {
			pterm.Success.Println("Chapter closed successfully")
		}
	}

	return nil
}
