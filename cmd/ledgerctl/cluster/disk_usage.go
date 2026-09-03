package cluster

import (
	"math"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// NewDiskUsageCommand creates the cluster disk-usage command.
func NewDiskUsageCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "disk-usage",
		Aliases:           []string{"du"},
		Short:             "Get disk usage",
		Long:              "Display filesystem-level disk usage on the connected node",
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runDiskUsage,
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
		{"VOLUME", "STATUS", "USED", "TOTAL", "AGE", "OBSERVED AT", "ERROR"},
		diskUsageVolumeRow("WAL", usage.GetWalVolume()),
		diskUsageVolumeRow("Data", usage.GetDataVolume()),
	}
	_ = pterm.DefaultTable.WithHasHeader(true).WithData(volumeData).Render()

	pterm.Println()
}

func diskUsageVolumeRow(name string, volume *clusterpb.VolumeUsage) []string {
	if volume == nil {
		return []string{name, "invalid", "0 B", "0 B", "-", "-", "missing volume"}
	}

	status := "invalid"
	if volume.GetValid() {
		status = "valid"
	}
	diagnostic := volume.GetError()
	if diagnostic == "" {
		diagnostic = "-"
	}

	return []string{
		name,
		status,
		cmdutil.FormatBytes(volume.GetUsedBytes()),
		cmdutil.FormatBytes(volume.GetTotalBytes()),
		formatDiskUsageAge(volume.GetSampleAgeMs(), volume.GetObservedAtUs()),
		formatDiskUsageObservedAt(volume.GetObservedAtUs()),
		diagnostic,
	}
}

func formatDiskUsageAge(ageMS, observedAtUS uint64) string {
	if observedAtUS == 0 {
		return "-"
	}
	if ageMS > uint64(math.MaxInt64/int64(time.Millisecond)) {
		return "invalid"
	}

	return (time.Duration(ageMS) * time.Millisecond).String()
}

func formatDiskUsageObservedAt(observedAtUS uint64) string {
	if observedAtUS == 0 || observedAtUS > math.MaxInt64 {
		return "-"
	}

	return time.UnixMicro(int64(observedAtUS)).UTC().Format(time.RFC3339Nano)
}
