package http

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
)

func TestPutKVHTTPOversizedValueErrorContract(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil)
	request := httptest.NewRequest(http.MethodPut, "/v3/ledger/kv/key", bytes.NewReader(make([]byte, domain.MaxKVValueBytes+1)))
	request.Header.Set("Content-Type", "application/octet-stream")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusRequestEntityTooLarge, response.Code)
	require.Equal(t, "application/json", response.Header().Get("Content-Type"))
	errorResponse := decodeResponse[ErrorResponse](t, response)
	require.Equal(t, "BODY_TOO_LARGE", errorResponse.ErrorCode)
	require.Equal(t, "KV value exceeds 1 MiB", errorResponse.ErrorMessage)
}
