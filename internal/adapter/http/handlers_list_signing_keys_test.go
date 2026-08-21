package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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

// TestHandleListSigningKeys_SortedByKeyID pins the ordering guarantee
// openapi.yml states, matching TestNewEventsSinksDTO_SortsByName on the sibling
// route. query.ReadSigningKeys returns map[string]SigningKeyEntry and
// ReadSigningKeysCursor builds its slice by ranging over it, so keys reach the
// converter in map-iteration order: without the sort, two identical requests
// returned the same set in a different array order. The input is supplied in
// reverse order so the sort cannot pass by accident.
func TestHandleListSigningKeys_SortedByKeyID(t *testing.T) {
	t.Parallel()

	keys := []*commonpb.SigningKey{
		{KeyId: "kc", PublicKey: []byte{0x03}, ParentKeyId: "kb"},
		{KeyId: "ka", PublicKey: []byte{0x01}},
		{KeyId: "kb", PublicKey: []byte{0x02}, ParentKeyId: "ka"},
	}

	out := newSigningKeyDTOList(keys)

	ids := make([]string, 0, len(out))
	for _, k := range out {
		ids = append(ids, k.KeyID)
	}

	require.Equal(t, []string{"ka", "kb", "kc"}, ids)

	// The sort must reorder whole elements, not just the id field.
	require.Equal(t, []string{"01", "02", "03"},
		[]string{out[0].PublicKey, out[1].PublicKey, out[2].PublicKey})
	require.Equal(t, []string{"", "ka", "kb"},
		[]string{out[0].ParentKeyID, out[1].ParentKeyID, out[2].ParentKeyID})
}

// The ordering must hold through the handler, not just the converter.
func TestHandleListSigningKeys_ResponseBodyIsSorted(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListSigningKeys(gomock.Any()).DoAndReturn(
		func(_ context.Context) (cursor.Cursor[*commonpb.SigningKey], error) {
			return cursor.NewSliceCursor([]*commonpb.SigningKey{
				{KeyId: "kc"},
				{KeyId: "ka"},
				{KeyId: "kb"},
			}), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/signing-keys", nil, nil)

	srv.handleListSigningKeys(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Less(t, strings.Index(body, `"keyId":"ka"`), strings.Index(body, `"keyId":"kb"`))
	require.Less(t, strings.Index(body, `"keyId":"kb"`), strings.Index(body, `"keyId":"kc"`))
}
