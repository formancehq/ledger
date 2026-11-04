package http

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
)

func TestOpenAPI_SignedRequestRepresentations(t *testing.T) {
	t.Parallel()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)
	cases := []struct{ method, path, request string }{
		{"POST", "/v3/{ledgerName}/transactions", `{"postings":[{"source":"world","destination":"bank","amount":100,"asset":"USD"}]}`},
		{"POST", "/v3/{ledgerName}/transactions/{transactionId}/revert", `{}`},
		{"POST", "/v3/{ledgerName}/transactions/{transactionId}/metadata", `{"reviewed":"yes"}`},
		{"DELETE", "/v3/{ledgerName}/transactions/{transactionId}/metadata/{key}", `null`},
		{"POST", "/v3/{ledgerName}/bulk", `[{"action":"CREATE_TRANSACTION","data":{"postings":[{"source":"world","destination":"bank","amount":100,"asset":"USD"}]}}]`},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			t.Parallel()
			operation := doc.Paths.Value(tc.path).GetOperation(tc.method)
			require.NotNil(t, operation.RequestBody)
			media := operation.RequestBody.Value.Content["application/vnd.formance.signed-request+json"]
			require.NotNil(t, media)
			var request any
			require.NoError(t, json.Unmarshal([]byte(tc.request), &request))
			body := map[string]any{"request": request, "signedBatch": map[string]any{"keyId": "key", "signature": base64.StdEncoding.EncodeToString(make([]byte, 64)), "payload": "AQ=="}}
			require.NoError(t, media.Schema.Value.VisitJSON(body))
			// A deletion has no business body. The generated client must send the
			// explicit null required by the server, rather than an empty object.
			if tc.method == "DELETE" {
				body["request"] = map[string]any{}
				require.Error(t, media.Schema.Value.VisitJSON(body))
				body["request"] = nil
			}
			delete(body, "signedBatch")
			require.Error(t, media.Schema.Value.VisitJSON(body), "signed transport cannot omit its proof")
		})
	}
}
