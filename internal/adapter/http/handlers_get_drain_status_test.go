package http

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

func TestHandleGetDrainStatus(t *testing.T) {
	t.Parallel()
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetDrainStatus(gomock.Any(), gomock.Any()).Return(&clusterpb.DrainStatus{
		LocalNode: 18446744073709551615, SupportsDrain: true, SupportsRelease: true,
		DrainEpoch: "rollout-7", DrainIndex: 10, LastPersistedIndex: 11, SyncStatus: "synced", ReleaseIndex: 0,
	}, nil)
	w := serveCluster(t, backend, http.MethodGet, "/v3/_/cluster/drain", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.JSONEq(t, `{"data":{
		"localNode":18446744073709551615,"supportsDrain":true,"drainEpoch":"rollout-7","drainIndex":10,
		"lastPersistedIndex":11,"syncStatus":"synced","releaseIndex":0,"supportsRelease":true
	}}`, w.Body.String())
}
