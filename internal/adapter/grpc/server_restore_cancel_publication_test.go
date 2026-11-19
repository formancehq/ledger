package grpc

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestCanceledDownloadCannotPublishCompletedStaging(t *testing.T) {
	t.Parallel()
	s := newServerForTest(t, nil)
	require.NoError(t, os.MkdirAll(s.stagingDir(), 0o750))
	staged, err := dal.OpenDirect(s.stagingDir(), s.logger)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	job := &downloadJob{readIndexGeneration: "generation-that-must-not-publish"}
	s.finishJob(ctx, job, nil, staged)
	require.Equal(t, restorepb.DownloadState_DOWNLOAD_STATE_CANCELED, job.state)
	require.False(t, s.downloaded)
	require.Nil(t, s.stagingStore)
	require.Empty(t, s.readIndexGeneration)
	require.NoDirExists(t, s.stagingDir())
}
