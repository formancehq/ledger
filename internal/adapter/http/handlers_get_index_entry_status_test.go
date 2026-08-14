package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleGetIndexEntryStatus_Success(t *testing.T) {
	t.Parallel()

	var capturedReq *servicepb.GetIndexEntryStatusRequest

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndexEntryStatus(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.GetIndexEntryStatusRequest) (*servicepb.IndexEntry, error) {
			capturedReq = req

			return &servicepb.IndexEntry{Ledger: req.GetLedger(), CurrentVersion: 3}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/metadata:TARGET_TYPE_ACCOUNT:color/status", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "metadata:TARGET_TYPE_ACCOUNT:color",
	})

	srv.handleGetIndexEntryStatus(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, capturedReq)
	require.Equal(t, "ledger1", capturedReq.GetLedger())
	require.Equal(t, "color", capturedReq.GetId().GetMetadata().GetKey())
}

// currentVersion 0 means "never built" and clients poll for > 0, so it must be
// present on the wire. Default protojson omitted it entirely.
func TestHandleGetIndexEntryStatus_MeaningfulZerosPresent(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndexEntryStatus(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.GetIndexEntryStatusRequest) (*servicepb.IndexEntry, error) {
			return &servicepb.IndexEntry{
				Ledger:         "ledger1",
				Cursor:         0,
				CurrentVersion: 0,
				PendingVersion: 0,
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/tx_builtin:TX_BUILTIN_INDEX_REFERENCE/status", nil,
		map[string]string{"ledgerName": "ledger1", "canonicalId": "tx_builtin:TX_BUILTIN_INDEX_REFERENCE"})

	srv.handleGetIndexEntryStatus(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	body := w.Body.String()
	require.Contains(t, body, `"currentVersion":0`)
	require.Contains(t, body, `"pendingVersion":0`)
	require.Contains(t, body, `"cursor":0`)
	// Unquoted, unlike protojson's fixed64.
	require.NotContains(t, body, `"cursor":"0"`)
}

func TestHandleGetIndexEntryStatus_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndexEntryStatus(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.GetIndexEntryStatusRequest) (*servicepb.IndexEntry, error) {
			return nil, commonpb.NewNotFoundError("not found")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP/status", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP",
	})

	srv.handleGetIndexEntryStatus(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleGetIndexEntryStatus_InvalidCanonical(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/bogus/status", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "bogus",
	})

	srv.handleGetIndexEntryStatus(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetIndexEntryStatus_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes/tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP/status", nil, map[string]string{
		"ledgerName":  "",
		"canonicalId": "tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP",
	})

	srv.handleGetIndexEntryStatus(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
