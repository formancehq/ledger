package http

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestHandleListAccounts_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAccounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ string, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Account], error) {
			return cursor.NewSliceCursor([]*commonpb.Account{
				{Address: "users:001"},
				{Address: "users:002"},
			}), nil
		}).AnyTimes()
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

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAccounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, pageSize uint32, afterAddress string, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Account], error) {
			capturedPageSize = pageSize
			capturedAfter = afterAddress

			return cursor.NewSliceCursor[*commonpb.Account](nil), nil
		}).AnyTimes()
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

// TestHandleListAccounts_PrefixFilterCanonicalReplacement proves the canonical
// replacement for the removed `prefix=` alias: an address-prefix selection
// passed through the generic `filter` as the textual `address ^= "<prefix>"`
// reaches the backend as the same AddressMatch_HardcodedPrefix the old alias
// produced, and the removed alias is no longer interpreted.
func TestHandleListAccounts_PrefixFilterCanonicalReplacement(t *testing.T) {
	t.Parallel()

	capture := func(t *testing.T, target string) *commonpb.QueryFilter {
		t.Helper()

		var captured *commonpb.QueryFilter

		backend := NewMockBackend(gomock.NewController(t))
		backend.EXPECT().ListAccounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, _ string, _ uint32, _ string, filter *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Account], error) {
				captured = filter

				return cursor.NewSliceCursor[*commonpb.Account](nil), nil
			}).AnyTimes()
		srv := newTestServer(t, backend)

		w := httptest.NewRecorder()
		r := newRequest(t, http.MethodGet, target, nil, map[string]string{"ledgerName": "ledger1"})
		srv.handleListAccounts(w, r)

		require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

		return captured
	}

	// Canonical textual replacement → a bare AddressMatch_HardcodedPrefix (the
	// sole filter is not wrapped in a redundant 1-element $and).
	fromFilter := capture(t, "/ledger1/accounts?filter="+url.QueryEscape(`address ^= "users:"`))
	require.NotNil(t, fromFilter)
	require.Equal(t, "users:", fromFilter.GetAddress().GetHardcodedPrefix(),
		"textual address-prefix filter must reach the backend as HardcodedPrefix")

	// The removed `prefix=` alias must no longer be interpreted: passed alone
	// (no `filter=`), it yields an unfiltered read (nil filter).
	aliasOnly := capture(t, "/ledger1/accounts?prefix=users:")
	require.Nil(t, aliasOnly, "the removed prefix= alias must not build a filter")
}

func TestHandleListAccounts_InvalidPageSize(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts?pageSize=abc", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListAccounts_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/accounts", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListAccounts_WithProfileHeader(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAccounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ string, _ uint32, _ string, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Account], error) {
			// Simulate what the real controller does: populate the profile from context
			if profile := query.ProfileFromContext(ctx); profile != nil {
				profile.IndexDuration = 2 * time.Millisecond
				profile.ItemsCollected = 1
			}

			return cursor.NewSliceCursor([]*commonpb.Account{
				{Address: "alice"},
			}), nil
		}).AnyTimes()
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

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAccounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ string, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Account], error) {
			return cursor.NewSliceCursor([]*commonpb.Account{
				{Address: "alice"},
			}), nil
		}).AnyTimes()
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
