package openapi_test

import (
	"encoding/json"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
)

// Validate the same wire fixtures accepted by QueryTemplate.Validate against
// both the maintained source and the artifact consumed by Stack.
func TestQueryTemplateWireContract(t *testing.T) {
	for _, path := range []string{"v2.yaml", "../openapi.yaml"} {
		t.Run(path, func(t *testing.T) {
			document, err := openapi3.NewLoader().LoadFromFile(path)
			require.NoError(t, err)
			template := document.Components.Schemas["V2QueryTemplate"].Value
			for _, fixture := range []string{
				`{"resource":"accounts","params":{"pageSize":15,"expand":["volumes"],"startTime":"2026-01-01T00:00:00Z","endTime":"2026-02-01T00:00:00Z"}}`,
				`{"resource":"volumes","params":{"groupBy":2,"insertionDate":true}}`,
			} {
				var value any
				require.NoError(t, json.Unmarshal([]byte(fixture), &value))
				require.NoError(t, template.VisitJSON(value))
			}
			require.Error(t, template.VisitJSON(map[string]any{"params": map[string]any{"resource": "accounts"}}))
			require.NotNil(t, document.Paths.Find("/v2/_info"))
			require.Nil(t, document.Paths.Find("/_/info"))
			response := document.Components.Schemas["V2ConfigInfoResponse"].Value
			require.NoError(t, response.VisitJSON(map[string]any{"data": map[string]any{"server": "ledger", "version": "v2.4.17", "config": map[string]any{}}}))
			require.Error(t, response.VisitJSON(map[string]any{"server": "ledger", "version": "v2.4.17", "config": map[string]any{}}))
		})
	}
}
