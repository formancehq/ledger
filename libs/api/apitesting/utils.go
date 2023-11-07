package apitesting

import (
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/stretchr/testify/require"
)

func ReadErrorResponse(t *testing.T, rec *httptest.ResponseRecorder) *api.ErrorResponse {
	t.Helper()
	ret := &api.ErrorResponse{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(ret))
	return ret
}

func ReadResponse[T any](t *testing.T, rec *httptest.ResponseRecorder, to T) {
	t.Helper()
	ret := &api.BaseResponse[T]{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(ret))
	reflect.ValueOf(to).Elem().Set(reflect.ValueOf(*ret.Data).Elem())
}

func ReadCursor[T any](t *testing.T, rec *httptest.ResponseRecorder, to *api.Cursor[T]) {
	t.Helper()
	ret := &api.BaseResponse[T]{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(ret))
	reflect.ValueOf(to).Elem().Set(reflect.ValueOf(ret.Cursor).Elem())
}
