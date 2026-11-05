package openapi_test

import (
	"encoding/json"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
)

// The server reads `resource` on the template and never inside `params`.
// Both the maintained source and the merged artifact must accept what the
// server stores and reject what it rejects.
func TestQueryTemplateWireContract(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"v2.yaml", "../openapi.yaml"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			document, err := openapi3.NewLoader().LoadFromFile(path)
			require.NoError(t, err)
			template := document.Components.Schemas["V2QueryTemplate"].Value

			for _, fixture := range []string{
				`{"resource":"accounts","params":{"pageSize":15,"sort":"address:asc","expand":["volumes"],"startTime":"2026-01-01T00:00:00Z","endTime":"2026-02-01T00:00:00Z"}}`,
				`{"resource":"transactions","params":{"pageSize":15}}`,
				`{"resource":"logs","body":{"$match":{"type":"NEW_TRANSACTION"}},"vars":{"x":{"type":"string"}}}`,
				`{"resource":"volumes","params":{"groupBy":2,"insertionDate":true}}`,
			} {
				var value any
				require.NoError(t, json.Unmarshal([]byte(fixture), &value))
				require.NoError(t, template.VisitJSON(value), fixture)
			}

			for _, fixture := range []string{
				`{"params":{"pageSize":1}}`,
				`{"resource":"nope"}`,
				`{"resource":"accounts","params":{"expand":"volumes"}}`,
			} {
				var value any
				require.NoError(t, json.Unmarshal([]byte(fixture), &value))
				require.Error(t, template.VisitJSON(value), fixture)
			}
		})
	}
}
