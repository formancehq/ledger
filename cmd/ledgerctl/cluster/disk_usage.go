package cluster

import (
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// NewDiskUsageCommand creates the cluster disk-usage command.
func NewDiskUsageCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "disk-usage",
		Aliases: []string{"du"},
		Short:   "Get disk usage",
		Long:    "Display filesystem-level disk usage on the connected node",
		RunE:    runDiskUsage,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runDiskUsage(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClusterClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	usage, err := client.GetDiskUsage(ctx, &clusterpb.GetDiskUsageRequest{})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to get disk usage", err)
	}

	if handled, err := cmdutil.EncodeStructured(cmd, usage); handled || err != nil {
		return err
	}

	displayDiskUsage(usage)

	return nil
}

func displayDiskUsage(usage *clusterpb.DiskUsage) {
	pterm.DefaultSection.Println("Volumes")

	volumeData := [][]string{
		{"VOLUME", "USED", "TOTAL"},
		{"WAL", cmdutil.FormatBytes(usage.GetWalVolume().GetUsedBytes()), cmdutil.FormatBytes(usage.GetWalVolume().GetTotalBytes())},
		{"Data", cmdutil.FormatBytes(usage.GetDataVolume().GetUsedBytes()), cmdutil.FormatBytes(usage.GetDataVolume().GetTotalBytes())},
	}
	_ = pterm.DefaultTable.WithHasHeader(true).WithData(volumeData).Render()

	pterm.Println()
}
