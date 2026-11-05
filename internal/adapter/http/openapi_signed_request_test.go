package http

import (
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
)

func TestOpenAPI_SignedRequestHeaders(t *testing.T) {
	t.Parallel()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)
	for _, tc := range []struct{ method, path string }{
		{"POST", "/v3/{ledgerName}/transactions"},
		{"POST", "/v3/{ledgerName}/transactions/{transactionId}/revert"},
		{"POST", "/v3/{ledgerName}/transactions/{transactionId}/metadata"},
		{"DELETE", "/v3/{ledgerName}/transactions/{transactionId}/metadata/{key}"},
		{"POST", "/v3/{ledgerName}/bulk"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			t.Parallel()
			operation := doc.Paths.Value(tc.path).GetOperation(tc.method)
			for _, header := range []string{"Signature", "Signature-Input", "Content-Digest", "Idempotency-Key"} {
				parameter := operation.Parameters.GetByInAndName("header", header)
				require.NotNil(t, parameter, header)
			}
			if tc.method == "DELETE" {
				require.Nil(t, operation.RequestBody)
			} else {
				require.NotNil(t, operation.RequestBody.Value.Content["application/json"])
				require.Len(t, operation.RequestBody.Value.Content, 1)
			}
		})
	}
}
