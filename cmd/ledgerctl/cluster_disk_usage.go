package main

import (
	"encoding/json"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newClusterDiskUsageCommand creates the cluster disk-usage command.
func newClusterDiskUsageCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "disk-usage",
		Aliases: []string{"du"},
		Short:   "Get disk usage",
		Long:    "Display disk space used by storage components on the connected node",
		RunE:    runClusterDiskUsage,
	}

	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runClusterDiskUsage(cmd *cobra.Command, _ []string) error {
	client, conn, err := getClusterClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := getContext(cmd)
	defer cancel()

	usage, err := client.GetDiskUsage(ctx, &clusterpb.GetDiskUsageRequest{})
	if err != nil {
		return fmt.Errorf("failed to get disk usage: %w", err)
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
		{pterm.Bold.Sprint("Component"), pterm.Bold.Sprint("Size")},
		{"Spool", formatBytes(uint64(usage.SpoolBytes))},
		{"WAL", formatBytes(uint64(usage.WalBytes))},
		{"Data", formatBytes(uint64(usage.DataBytes))},
	}
	_ = pterm.DefaultTable.WithHasHeader(true).WithData(componentData).Render()
	pterm.Println()

	pterm.DefaultSection.Println("Volumes")

	volumeData := [][]string{
		{pterm.Bold.Sprint("Volume"), pterm.Bold.Sprint("Used"), pterm.Bold.Sprint("Total")},
		{"WAL", formatBytes(uint64(usage.WalVolumeBytes)), formatBytes(uint64(usage.WalVolumeTotalBytes))},
		{"Data", formatBytes(uint64(usage.DataVolumeBytes)), formatBytes(uint64(usage.DataVolumeTotalBytes))},
	}
	_ = pterm.DefaultTable.WithHasHeader(true).WithData(volumeData).Render()
	pterm.Println()
}
