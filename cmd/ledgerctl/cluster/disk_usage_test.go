package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

func TestDiskUsageVolumeRow(t *testing.T) {
	t.Parallel()

	observedAt := time.Date(2026, time.September, 3, 12, 0, 0, 123000000, time.UTC)
	row := diskUsageVolumeRow("WAL", &clusterpb.VolumeUsage{
		UsedBytes:    10,
		TotalBytes:   100,
		ObservedAtUs: uint64(observedAt.UnixMicro()),
		SampleAgeMs:  2500,
		Valid:        true,
	})
	require.Equal(t, "WAL", row[0])
	require.Equal(t, "valid", row[1])
	require.Equal(t, "2.5s", row[4])
	require.Equal(t, observedAt.Format(time.RFC3339Nano), row[5])
	require.Equal(t, "-", row[6])

	invalid := diskUsageVolumeRow("Data", &clusterpb.VolumeUsage{Error: "input/output error"})
	require.Equal(t, "invalid", invalid[1])
	require.Equal(t, "-", invalid[4])
	require.Equal(t, "-", invalid[5])
	require.Equal(t, "input/output error", invalid[6])
}
