package http

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

func TestHandlePreviewRestore(t *testing.T) {
	t.Parallel()
	backend := NewMockRestoreBackend(gomock.NewController(t))
	backend.EXPECT().PreviewRestore(gomock.Any(), gomock.Any()).Return(&restorepb.PreviewRestoreResponse{
		LastAppliedIndex: 42, LastAppliedTimestamp: 1700000000000000, LastSequence: 9,
		LedgerCount: 2, LedgerNames: []string{"a", "b"},
	}, nil)
	w := serveRestore(t, backend, http.MethodGet, "/v3/_/restore/preview", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.JSONEq(t, `{"data":{"lastAppliedIndex":42,"lastAppliedTimestamp":1700000000000000,"lastSequence":9,
		"ledgerCount":2,"ledgerNames":["a","b"]}}`, w.Body.String())
}
