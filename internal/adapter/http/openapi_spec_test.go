package http

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestOpenAPISpec_NoBareObjects(t *testing.T) {
	t.Parallel()

	spec, err := os.ReadFile("../../../openapi.yml")
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, yaml.Unmarshal(spec, &doc))

	var walk func(any, string)
	walk = func(value any, path string) {
		switch node := value.(type) {
		case map[string]any:
			if node["type"] == "object" && path != "components/schemas/DropAction" {
				_, hasAdditionalProperties := node["additionalProperties"]
				hasShape := false
				for _, key := range []string{"properties", "allOf", "anyOf", "oneOf"} {
					switch shape := node[key].(type) {
					case map[string]any:
						hasShape = hasShape || len(shape) > 0
					case []any:
						hasShape = hasShape || len(shape) > 0
					}
				}
				// SDK generators can strip every field from an implicit object.
				// DropAction intentionally carries no fields; all other objects
				// must describe their shape or an additional-properties policy.
				if !hasShape && !hasAdditionalProperties {
					t.Errorf("%s is a bare object schema", path)
				}
			}
			for key, child := range node {
				// Example and default values are payloads, not schemas.
				if key == "example" || key == "examples" || key == "default" || key == "enum" {
					continue
				}
				childPath := key
				if path != "" {
					childPath = path + "/" + key
				}
				walk(child, childPath)
			}
		case []any:
			for i, child := range node {
				walk(child, fmt.Sprintf("%s/%d", path, i))
			}
		}
	}
	walk(doc, "")
}

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

func TestOpenAPISpec_SystemLogPayloadPreservation(t *testing.T) {
	t.Parallel()

	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)
	payload := doc.Components.Schemas["SystemLog"].Value.Properties["payload"].Value
	require.NotNil(t, payload.AdditionalProperties.Has, "system payload sibling variants must survive SDK decoding")
	require.True(t, *payload.AdditionalProperties.Has)
	require.Same(t, doc.Components.Schemas["LedgerLog"].Value, payload.Properties["apply"].Value.Properties["log"].Value)

	fixturesJSON, err := os.ReadFile("../../../tests/sdk/system-log-payloads.json")
	require.NoError(t, err)
	var fixtures map[string]map[string]any
	require.NoError(t, json.Unmarshal(fixturesJSON, &fixtures))

	// Reconcile the SDK fixtures with the live descriptor and the actual custom
	// JSON codec. A new variant or a JSON-name change must extend the regression.
	fields := (&commonpb.LogPayload{}).ProtoReflect().Descriptor().Oneofs().ByName("type").Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		t.Run(field.JSONName(), func(t *testing.T) {
			t.Parallel()
			fixture, ok := fixtures[field.JSONName()]
			require.True(t, ok, "missing generated SDK fixture")
			require.Contains(t, fixture, field.JSONName())
			require.Len(t, fixture, 1)
			require.NoError(t, payload.VisitJSON(fixture))

			message := (&commonpb.LogPayload{}).ProtoReflect()
			message.Set(field, protoreflect.ValueOfMessage(message.NewField(field).Message()))
			if field.JSONName() != "apply" {
				// Non-apply fixtures also round-trip through the real wire codec.
				// apply.log is an output-only projection, not a protojson input.
				fixtureJSON, err := json.Marshal(fixture)
				require.NoError(t, err)
				require.NoError(t, protojson.Unmarshal(fixtureJSON, message.Interface()))
			}
			encoded, err := json.Marshal(message.Interface())
			require.NoError(t, err)
			var actual map[string]any
			require.NoError(t, json.Unmarshal(encoded, &actual))
			require.Contains(t, actual, field.JSONName())
			require.Len(t, actual, 1)
			if field.JSONName() != "apply" {
				require.Equal(t, fixture, actual)
			}
		})
	}
	// Two additional fixtures protect metadata apply data and a future sibling.
	require.Len(t, fixtures, fields.Len()+2)
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
