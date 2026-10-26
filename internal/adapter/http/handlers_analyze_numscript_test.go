package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/application/numscriptanalysis"
)

func TestHandleAnalyzeNumscript(t *testing.T) {
	t.Parallel()
	server := &Server{}
	requestBody := `{"source":"vars { account $target } send [USD/2 1] (source = @world destination = $target)","version":"v1"}`
	for range 2 {
		req := httptest.NewRequest(http.MethodPost, "/v3/numscripts/analyze", strings.NewReader(requestBody))
		response := httptest.NewRecorder()
		server.handleAnalyzeNumscript(response, req)
		require.Equal(t, http.StatusOK, response.Code)
		result := decodeResponse[BaseResponse[numscriptanalysis.Result]](t, response)
		require.Equal(t, []numscriptanalysis.Input{{Name: "target", Type: "account"}}, result.Data.Inputs)
		require.Empty(t, result.Data.Diagnostics)
	}
}
