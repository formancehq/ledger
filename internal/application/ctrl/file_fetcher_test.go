package ctrl

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

func TestFileFetcher_FetchFileOnceRejectsNonLocalPath(t *testing.T) {
	t.Parallel()

	client, clientState := newMockSnapshotClient(t, nil, nil, nil)
	fetcher := &fileFetcher{sessionID: "test-session"}

	err := fetcher.fetchFileOnce(t.Context(), client, &snapshotpb.FileEntry{Path: "../outside"}, t.TempDir(), nil)
	require.ErrorContains(t, err, "invalid snapshot path")
	require.Zero(t, clientState.fetchFileCalls.Load())
}

func TestFileFetcher_FetchFileClassifiesClientResolutionFailure(t *testing.T) {
	t.Parallel()

	resolutionErr := errors.New("no connection to peer 2")
	fetcher := &fileFetcher{
		clientSource: func() (snapshotpb.SnapshotServiceClient, error) {
			return nil, resolutionErr
		},
		maxRetries: 1,
		sessionID:  "test-session",
	}

	err := fetcher.fetchFile(t.Context(), &snapshotpb.FileEntry{Path: "data.bin"}, t.TempDir(), nil)
	require.ErrorIs(t, err, state.ErrNotAvailable)
	require.ErrorIs(t, err, resolutionErr)
}
