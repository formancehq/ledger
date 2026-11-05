package http

import (
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
			require.NotNil(t, operation.Responses.Value("415"), "signing headers without JSON content type return 415")
			for _, name := range []string{"Formance-Signing-Key-Id", "Formance-Signature"} {
				count := 0
				for _, ref := range operation.Parameters {
					parameter := ref.Value
					if parameter.In == "header" && parameter.Name == name {
						count++
						require.False(t, parameter.Required, "unsigned JSON must remain supported")
						require.Equal(t, "string", parameter.Schema.Value.Type.Slice()[0])
					}
				}
				require.Equal(t, 1, count, "signing header must be documented exactly once: %s", name)
			}
			require.NotNil(t, operation.RequestBody)
			require.Len(t, operation.RequestBody.Value.Content, 1, "both representations use ordinary JSON")
			media := operation.RequestBody.Value.Content["application/json"]
			require.NotNil(t, media)
			var request any
			require.NoError(t, json.Unmarshal([]byte(tc.request), &request))
			signedSchema := media.Schema.Value
			if tc.method == "DELETE" {
				require.False(t, operation.RequestBody.Value.Required, "unsigned metadata deletion needs no body")
			} else {
				require.Len(t, media.Schema.Value.AnyOf, 2)
				require.NoError(t, media.Schema.Value.AnyOf[0].Value.VisitJSON(request), "ordinary REST body remains valid")
				signedSchema = media.Schema.Value.AnyOf[1].Value
			}
			body := map[string]any{"request": request, "payload": "AQ=="}
			require.NoError(t, media.Schema.Value.VisitJSON(body))
			for _, name := range []string{"keyId", "signature", "signedBatch"} {
				body[name] = "unexpected"
				require.Error(t, signedSchema.VisitJSON(body), "signature metadata belongs in headers")
				delete(body, name)
			}
			// A deletion has no business body. The generated client must send the
			// explicit null required by the server, rather than an empty object.
			if tc.method == "DELETE" {
				body["request"] = map[string]any{}
				require.Error(t, signedSchema.VisitJSON(body))
				body["request"] = nil
			}
			delete(body, "payload")
			require.Error(t, signedSchema.VisitJSON(body), "signed transport cannot omit its proof")
			body["payload"] = ""
			require.Error(t, signedSchema.VisitJSON(body), "signed payload must be nonempty")
		})
	}
}
