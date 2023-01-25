package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/api"
	"github.com/stretchr/testify/require"
)

func createJSONBuffer(t *testing.T, v any) io.Reader {
	data, err := json.Marshal(v)
	require.NoError(t, err)

	return bytes.NewBuffer(data)
}

func readTestResponse[T any](t *testing.T, recorder *httptest.ResponseRecorder) T {
	body := api.BaseResponse[T]{}
	require.NoError(t, json.NewDecoder(recorder.Body).Decode(&body))
	return *body.Data
}
