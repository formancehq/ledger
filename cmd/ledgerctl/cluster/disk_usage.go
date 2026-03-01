package cluster

import (
	"encoding/json"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewDiskUsageCommand creates the cluster disk-usage command.
func NewDiskUsageCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "disk-usage",
		Aliases: []string{"du"},
		Short:   "Get disk usage",
		Long:    "Display disk space used by storage components on the connected node",
		RunE:    runDiskUsage,
	}

	cmd.Flags().Bool("json", false, "Output as JSON")
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

	jsonMode, _ := cmd.Flags().GetBool("json")
	if jsonMode {
		data, err := json.MarshalIndent(usage, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal response: %w", err)
		}
		fmt.Println(string(data))
		return nil
	}

	displayDiskUsage(usage)
	return nil
}

func displayDiskUsage(usage *clusterpb.DiskUsage) {
	pterm.DefaultSection.Println("Storage Components")

	componentData := [][]string{
		{"COMPONENT", "SIZE"},
		{"Spool", cmdutil.FormatBytes(uint64(usage.SpoolBytes))},
		{"WAL", cmdutil.FormatBytes(uint64(usage.WalBytes))},
		{"Data", cmdutil.FormatBytes(uint64(usage.DataBytes))},
	}
	_ = pterm.DefaultTable.WithHasHeader(true).WithData(componentData).Render()
	pterm.Println()

	pterm.DefaultSection.Println("Volumes")

	volumeData := [][]string{
		{"VOLUME", "USED", "TOTAL"},
		{"WAL", cmdutil.FormatBytes(uint64(usage.WalVolumeBytes)), cmdutil.FormatBytes(uint64(usage.WalVolumeTotalBytes))},
		{"Data", cmdutil.FormatBytes(uint64(usage.DataVolumeBytes)), cmdutil.FormatBytes(uint64(usage.DataVolumeTotalBytes))},
	}
	_ = pterm.DefaultTable.WithHasHeader(true).WithData(volumeData).Render()
	pterm.Println()
}
