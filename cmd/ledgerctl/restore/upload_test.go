package restore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

// scriptedStatusClient reports a RUNNING download for runningPolls calls, then
// SUCCEEDED.
type scriptedStatusClient struct {
	restorepb.RestoreServiceClient

	runningPolls int
	calls        int
}

func (c *scriptedStatusClient) GetDownloadStatus(
	context.Context,
	*restorepb.GetDownloadStatusRequest,
	...grpc.CallOption,
) (*restorepb.GetDownloadStatusResponse, error) {
	c.calls++

	const totalBytes = 10 * 1024 * 1024

	if c.calls > c.runningPolls {
		return &restorepb.GetDownloadStatusResponse{
			State:           restorepb.DownloadState_DOWNLOAD_STATE_SUCCEEDED,
			TotalBytes:      totalBytes,
			BytesDownloaded: totalBytes,
			TotalFiles:      2,
			FilesDownloaded: 2,
		}, nil
	}

	return &restorepb.GetDownloadStatusResponse{
		State:           restorepb.DownloadState_DOWNLOAD_STATE_RUNNING,
		TotalBytes:      totalBytes,
		BytesDownloaded: uint64(c.calls) * totalBytes / uint64(c.runningPolls+1),
		TotalFiles:      2,
		FilesDownloaded: 1,
		CurrentFile:     "data.sst",
	}, nil
}

// TestPollUntilTerminalHasNoProgressRace is meaningful under -race: the
// download outlasts pterm's one-second elapsed-time redraw, so any progress
// bar that redraws from a goroutine of its own reads the fields each poll
// writes.
func TestPollUntilTerminalHasNoProgressRace(t *testing.T) {
	t.Parallel()

	client := &scriptedStatusClient{runningPolls: 10}

	resp, err := pollUntilTerminal(context.Background(), client, "job", 300*time.Millisecond, time.Second)
	require.NoError(t, err)
	require.Equal(t, restorepb.DownloadState_DOWNLOAD_STATE_SUCCEEDED, resp.GetState())
	require.Equal(t, 11, client.calls)
}
