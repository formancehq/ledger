package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleListSigningKeys_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListSigningKeys(gomock.Any()).DoAndReturn(
		func(_ context.Context) (cursor.Cursor[*commonpb.SigningKey], error) {
			return cursor.NewSliceCursor([]*commonpb.SigningKey{
				{KeyId: "k1"},
			}), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/signing-keys", nil, nil)

	srv.handleListSigningKeys(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

// The public key is hex, not base64: every byte field in this codebase is hex
// (Chapter.MarshalJSON, AuditEntry.hash) and `ledgerctl signing list-keys`
// already prints a "PUBLIC KEY (HEX)" column.
func TestHandleListSigningKeys_PublicKeyIsHex(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListSigningKeys(gomock.Any()).DoAndReturn(
		func(_ context.Context) (cursor.Cursor[*commonpb.SigningKey], error) {
			return cursor.NewSliceCursor([]*commonpb.SigningKey{{
				KeyId:       "k1",
				PublicKey:   []byte{0xde, 0xad, 0xbe, 0xef},
				ParentKeyId: "",
			}}), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/signing-keys", nil, nil)

	srv.handleListSigningKeys(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	body := w.Body.String()
	require.Contains(t, body, `"publicKey":"deadbeef"`)
	// base64 of the same bytes must not appear.
	require.NotContains(t, body, "3q2+7w==")
	require.Contains(t, body, `"keyId":"k1"`)
	// "" = root key is a real value, so the field is present, not omitted.
	require.Contains(t, body, `"parentKeyId":""`)
	require.NotContains(t, body, "public_key")
	require.NotContains(t, body, "parent_key_id")
}

// An empty list must serialize as [] rather than null.
func TestHandleListSigningKeys_EmptyIsArrayNotNull(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListSigningKeys(gomock.Any()).DoAndReturn(
		func(_ context.Context) (cursor.Cursor[*commonpb.SigningKey], error) {
			return cursor.NewSliceCursor([]*commonpb.SigningKey{}), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/signing-keys", nil, nil)

	srv.handleListSigningKeys(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"data":[]`)
}

func TestHandleListSigningKeys_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListSigningKeys(gomock.Any()).DoAndReturn(
		func(_ context.Context) (cursor.Cursor[*commonpb.SigningKey], error) {
			return nil, errors.New("boom")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/signing-keys", nil, nil)

	srv.handleListSigningKeys(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}
