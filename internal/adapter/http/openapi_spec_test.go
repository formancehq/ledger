package http

import (
	"os"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

// TestOpenAPISpec_DecodesStrictly fails when openapi.yml repeats a mapping key.
// A duplicate is not a style issue: strict parsers reject the whole document,
// and lenient ones silently keep only the last definition, so the published
// contract depends on which tool reads it. POST /v3/{ledgerName}/promote once
// carried two '400' responses.
func TestOpenAPISpec_DecodesStrictly(t *testing.T) {
	t.Parallel()

	spec, err := os.ReadFile("../../../openapi.yml")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, yaml.Unmarshal(spec, &doc))
	require.Contains(t, doc, "paths")
}

func TestOpenAPISpec_LogPayloadPreservation(t *testing.T) {
	t.Parallel()

	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)
	data := doc.Components.Schemas["LedgerLog"].Value.Properties["data"].Value
	// OAS defaults to allowing properties, but SDK generators can interpret an
	// implicit object as an empty model and silently erase operation payloads.
	require.NotNil(t, data.AdditionalProperties.Has, "log payloads must explicitly allow additional properties")
	require.True(t, *data.AdditionalProperties.Has)
	for _, payload := range []any{
		map[string]any{"transaction": map[string]any{"id": float64(7)}, "accountMetadata": map[string]any{"bank": map[string]any{"owner": "customer"}}},
		map[string]any{"targetType": "TRANSACTION", "targetId": float64(0), "metadata": map[string]any{"checked": true, "missing": nil}},
		map[string]any{"reason": "already applied", "context": map[string]any{"id": "7"}},
		map[string]any{"originalId": "18446744073709551615"},
	} {
		require.NoError(t, data.VisitJSON(payload))
	}
}

func TestOpenAPISpec_PreparedQueryFilterInputs(t *testing.T) {
	t.Parallel()

	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)
	require.NoError(t, doc.Components.Schemas["PreparedQueryFilterInput"].Value.Validate(t.Context()))
	for _, alternative := range doc.Components.Schemas["PreparedQueryFilterInput"].Value.OneOf {
		if alternative.Value.Type.Is("object") {
			// A null-only object also passes instance validation, but generators
			// turn it into an empty model whose helper erases structured filters.
			require.NotEmpty(t, alternative.Value.Properties, "object filter alternatives must describe QueryFilter operators")
		}
	}
	for _, name := range []string{"CreatePreparedQueryRequest", "UpdatePreparedQueryRequest"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			request := doc.Components.Schemas[name].Value
			filter := request.Properties["filter"].Value
			for _, value := range []any{
				map[string]any{"$match": map[string]any{"reference": "invoice"}},
				map[string]any{"$and": []any{
					map[string]any{"$match": map[string]any{"reference": "invoice"}},
					map[string]any{"$not": map[string]any{"$match": map[string]any{"reverted": true}}},
				}},
				`metadata[status] == "active"`,
				nil,
			} {
				require.NoError(t, filter.VisitJSON(value), "%s filter %#v", name, value)
			}
			for _, value := range []any{map[string]any{}, []any{}, float64(7), true} {
				require.Error(t, filter.VisitJSON(value), "%s filter %#v", name, value)
			}
			// Optional update filters must distinguish omission from explicit null.
			withoutFilter := map[string]any{}
			if name == "CreatePreparedQueryRequest" {
				withoutFilter = map[string]any{"name": "by-reference", "target": "TRANSACTIONS"}
			}
			require.NoError(t, request.VisitJSON(withoutFilter))
			withoutFilter["filter"] = nil
			require.NoError(t, request.VisitJSON(withoutFilter))
		})
	}
}
