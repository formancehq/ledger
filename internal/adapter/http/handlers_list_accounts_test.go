package http

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestHandleListAccounts_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		listAccountsFn: func(_ context.Context, _ string, _ uint32, _ string, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Account], error) {
			return cursor.NewSliceCursor([]*commonpb.Account{
				{Address: "users:001"},
				{Address: "users:002"},
			}), nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListAccounts_WithPagination(t *testing.T) {
	t.Parallel()

	var (
		capturedPageSize uint32
		capturedAfter    string
	)

	backend := &mockBackend{
		listAccountsFn: func(_ context.Context, _ string, pageSize uint32, afterAddress string, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Account], error) {
			capturedPageSize = pageSize
			capturedAfter = afterAddress

			return cursor.NewSliceCursor[*commonpb.Account](nil), nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts?pageSize=10&after=users:005", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, uint32(10), capturedPageSize)
	require.Equal(t, "users:005", capturedAfter)
}

func TestHandleListAccounts_InvalidPageSize(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts?pageSize=abc", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListAccounts_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/accounts", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListAccounts_WithProfileHeader(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		listAccountsFn: func(ctx context.Context, _ string, _ uint32, _ string, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Account], error) {
			// Simulate what the real controller does: populate the profile from context
			if profile := query.ProfileFromContext(ctx); profile != nil {
				profile.IndexDuration = 2 * time.Millisecond
				profile.ItemsCollected = 1
			}

			return cursor.NewSliceCursor([]*commonpb.Account{
				{Address: "alice"},
			}), nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts", nil, map[string]string{
		"ledgerName": "ledger1",
	})
	r.Header.Set("X-Query-Profile", "true")

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	// Profile header should be present
	profileHeader := w.Header().Get("X-Query-Profile-Result")
	require.NotEmpty(t, profileHeader, "X-Query-Profile-Result header should be set when requested")

	// Decode and verify it's valid protobuf with the values set by the mock
	data, err := base64.StdEncoding.DecodeString(profileHeader)
	require.NoError(t, err)

	var pb servicepb.QueryProfile
	require.NoError(t, proto.Unmarshal(data, &pb))
	assert.Equal(t, int64(2000), pb.GetIndexDurationUs())
	assert.Equal(t, int32(1), pb.GetItemsCollected())
}

func TestHandleListAccounts_WithoutProfileHeader(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		listAccountsFn: func(_ context.Context, _ string, _ uint32, _ string, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Account], error) {
			return cursor.NewSliceCursor([]*commonpb.Account{
				{Address: "alice"},
			}), nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts", nil, map[string]string{
		"ledgerName": "ledger1",
	})
	// No X-Query-Profile header

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Header().Get("X-Query-Profile-Result"), "X-Query-Profile-Result should not be set when not requested")
}
