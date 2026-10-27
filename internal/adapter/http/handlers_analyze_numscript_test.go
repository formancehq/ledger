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

func TestHandleAnalyzeNumscriptRejectsTrailingAndOversizedBodies(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"second value", `{"source":"send [USD/2 1] (source = @world destination = @alice)","version":"v1"}{}`, http.StatusBadRequest},
		{"trailing junk", `{"source":"send [USD/2 1] (source = @world destination = @alice)","version":"v1"}junk`, http.StatusBadRequest},
		{"oversized suffix", `{"source":"send [USD/2 1] (source = @world destination = @alice)","version":"v1"}` + strings.Repeat(" ", int(defaultMaxBodySize)), http.StatusRequestEntityTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			request := httptest.NewRequest(http.MethodPost, "/v3/numscripts/analyze", strings.NewReader(tc.body))
			response := httptest.NewRecorder()
			request.Body = http.MaxBytesReader(response, request.Body, defaultMaxBodySize)
			(&Server{}).handleAnalyzeNumscript(response, request)
			require.Equal(t, tc.status, response.Code)
		})
	}
}
