package chapters

import (
	"fmt"
	"strconv"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewArchiveCommand creates the chapters archive command.
func NewArchiveCommand() *cobra.Command {
	return &cobra.Command{
		Use:               "archive <chapter-id>",
		Short:             "Archive a closed chapter to cold storage",
		Long:              "Archive a closed chapter's logs and audit entries to cold storage, then purge them from hot storage.",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runArchive,
	}
}

func runArchive(cmd *cobra.Command, args []string) error {
	chapterID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid chapter ID %q: %w", args[0], err)
	}

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Envelopes: servicepb.UnsignedEnvelopes(&servicepb.Request{
			Type: &servicepb.Request_ArchiveChapter{
				ArchiveChapter: &servicepb.ArchiveChapterRequest{
					ChapterId: chapterID,
				},
			},
		}),
	})
	if err != nil {
		return fmt.Errorf("archiving chapter: %w", err)
	}

	if len(resp.GetLogs()) > 0 {
		log := resp.GetLogs()[0]
		if archiveLog := log.GetPayload().GetArchiveChapter(); archiveLog != nil {
			pterm.Success.Printfln("Chapter %d archival initiated", archiveLog.GetChapter().GetId())
			pterm.Info.Println("Background archiver will export data to cold storage and confirm")
		} else {
			pterm.Success.Println("Chapter archival initiated")
		}
	}

	return nil
}
