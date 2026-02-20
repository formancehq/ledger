package periods

import (
	"fmt"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewArchiveCommand creates the periods archive command.
func NewArchiveCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "archive <period-id>",
		Short: "Archive a closed period to cold storage",
		Long:  "Archive a closed period's logs and audit entries to cold storage, then purge them from hot storage.",
		Args:  cobra.ExactArgs(1),
		RunE:  runArchive,
	}
}

func runArchive(cmd *cobra.Command, args []string) error {
	periodID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid period ID %q: %w", args[0], err)
	}

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{
			{
				Type: &servicepb.Request_ArchivePeriod{
					ArchivePeriod: &servicepb.ArchivePeriodRequest{
						PeriodId: periodID,
					},
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("archiving period: %w", err)
	}

	if len(resp.Logs) > 0 {
		log := resp.Logs[0]
		if archiveLog := log.Payload.GetArchivePeriod(); archiveLog != nil {
			pterm.Success.Printfln("Period %d archival initiated", archiveLog.Period.Id)
			pterm.Info.Println("Background archiver will export data to cold storage and confirm")
		} else {
			pterm.Success.Println("Period archival initiated")
		}
	}

	return nil
}
