package ctrl

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

func TestFileFetcher_FetchFileOnceRejectsNonLocalPath(t *testing.T) {
	t.Parallel()

	client, clientState := newMockSnapshotClient(t, nil, nil, nil)
	fetcher := &fileFetcher{client: client, sessionID: "test-session"}

	err := fetcher.fetchFileOnce(t.Context(), &snapshotpb.FileEntry{Path: "../outside"}, t.TempDir(), nil)
	require.ErrorContains(t, err, "invalid snapshot path")
	require.Zero(t, clientState.fetchFileCalls.Load())
}
