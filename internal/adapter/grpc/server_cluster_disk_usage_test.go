package grpc

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/monitoring/diskusage"
)

func TestVolumeUsageResponse(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	observedAt := now.Add(-12*time.Second - 345*time.Millisecond)

	response := volumeUsageResponse(diskusage.VolumeSample{
		UsedBytes:  10,
		TotalBytes: 100,
		ObservedAt: observedAt,
		Valid:      true,
	}, now)

	require.Equal(t, uint64(10), response.GetUsedBytes())
	require.Equal(t, uint64(100), response.GetTotalBytes())
	require.Equal(t, uint64(observedAt.UnixMicro()), response.GetObservedAtUs())
	require.Equal(t, uint64(12345), response.GetSampleAgeMs())
	require.True(t, response.GetValid())
	require.Empty(t, response.GetError())
}

func TestVolumeUsageResponsePreservesInvalidDiagnostics(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	observedAt := now.Add(-time.Minute)
	err := errors.New("input/output error")

	response := volumeUsageResponse(diskusage.VolumeSample{
		UsedBytes:  10,
		TotalBytes: 100,
		ObservedAt: observedAt,
		Valid:      false,
		Error:      err.Error(),
	}, now)

	require.Equal(t, uint64(observedAt.UnixMicro()), response.GetObservedAtUs())
	require.Equal(t, uint64(time.Minute.Milliseconds()), response.GetSampleAgeMs())
	require.False(t, response.GetValid())
	require.Equal(t, err.Error(), response.GetError())
}

func TestVolumeUsageResponseClampsFutureSampleAge(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	response := volumeUsageResponse(diskusage.VolumeSample{
		ObservedAt: now.Add(time.Second),
		Valid:      true,
	}, now)

	require.Zero(t, response.GetSampleAgeMs())
}
