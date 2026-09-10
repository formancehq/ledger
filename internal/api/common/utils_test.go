package common

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeBodyLimit(t *testing.T) {
	t.Parallel()

	t.Run("accepts exact limit", func(t *testing.T) {
		t.Parallel()

		body := `"` + strings.Repeat("a", int(MaxJSONBodySize)-2) + `"`
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		response := httptest.NewRecorder()
		var value string

		require.True(t, DecodeBody(response, request, &value))
		require.Len(t, value, int(MaxJSONBodySize)-2)
	})

	t.Run("rejects over limit", func(t *testing.T) {
		t.Parallel()

		body := `"` + strings.Repeat("a", int(MaxJSONBodySize)-1) + `"`
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		response := httptest.NewRecorder()
		var value string

		require.False(t, DecodeBody(response, request, &value))
		require.Equal(t, http.StatusRequestEntityTooLarge, response.Code)
		require.Contains(t, response.Body.String(), ErrRequestBodyTooLarge)
	})
}
