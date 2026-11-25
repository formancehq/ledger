package http

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

func TestHandleTransferLeadership(t *testing.T) {
	t.Parallel()
	for name, body := range map[string]string{
		"number": `{"transferee":18446744073709551615}`,
		"string": `{"transferee":"18446744073709551615"}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().TransferLeadership(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *clusterpb.TransferLeadershipRequest) (*clusterpb.TransferLeadershipResponse, error) {
				require.Equal(t, uint64(18446744073709551615), req.GetTransferee())

				return &clusterpb.TransferLeadershipResponse{NewLeader: req.GetTransferee()}, nil
			})
			w := serveCluster(t, backend, http.MethodPost, "/v3/_/cluster/leadership/transfer", body)
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.JSONEq(t, `{"data":{"newLeader":18446744073709551615}}`, w.Body.String())
		})
	}
}

func TestHandleTransferLeadership_InvalidTransferee(t *testing.T) {
	t.Parallel()
	for name, body := range map[string]string{
		"missing": `{}`,
		"null":    `{"transferee":null}`,
		"zero":    `{"transferee":0}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			w := serveCluster(t, NewMockBackend(gomock.NewController(t)), http.MethodPost, "/v3/_/cluster/leadership/transfer", body)
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		})
	}
}
