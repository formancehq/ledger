package http

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestOpenAPIAtomicApplyCommandCoverage(t *testing.T) {
	t.Parallel()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)
	visited := map[protoreflect.FullName]bool{}
	var inspect func(protoreflect.MessageDescriptor)
	inspect = func(message protoreflect.MessageDescriptor) {
		if visited[message.FullName()] {
			return
		}
		visited[message.FullName()] = true
		switch message.FullName() {
		case "common.Uint256", "common.BigUint", "common.SignedBigInt", "common.Timestamp":
			return // These scalar messages use the documented HTTP representation.
		}
		name := "AtomicApply" + string(message.Name())
		ref := doc.Components.Schemas[name]
		require.NotNil(t, ref, name)
		schema := ref.Value
		require.NoError(t, schema.Validate(t.Context()), name)
		require.NotNil(t, schema.AdditionalProperties.Has, name)
		require.False(t, *schema.AdditionalProperties.Has, name)
		require.Len(t, schema.Properties, message.Fields().Len(), name)
		for i := range message.Fields().Len() {
			field := message.Fields().Get(i)
			require.Contains(t, schema.Properties, field.JSONName(), field.FullName())
			if field.IsMap() {
				field = field.MapValue()
			}
			if field.Kind() == protoreflect.MessageKind {
				inspect(field.Message())
			}
		}
	}
	inspect((&servicepb.Request{}).ProtoReflect().Descriptor())

	operation := doc.Paths.Value("/v3/_/apply").Post
	require.NotNil(t, operation)
	for _, parameter := range operation.Parameters {
		require.NotEqual(t, "query", parameter.Value.In, "atomic Apply has no mode selectors")
	}
	request := operation.RequestBody.Value.Content["application/json"].Schema.Value
	for _, fixture := range []struct {
		body  string
		valid bool
	}{
		{`{"requests":[{"createLedger":{"name":"new","metadata":{"tag":{"stringValue":"value"}}}},{"saveLedgerMetadata":{"ledger":"existing","metadata":{"count":{"intValue":"9007199254740993"}}}}]}`, true},
		{`{"requests":[{"apply":{"ledger":"main","action":{"createTransaction":{"postings":[{"source":"world","destination":"bank","asset":"USD","amount":"9007199254740993"}],"timestamp":"2026-01-01T00:00:00Z"}}}}]}`, true},
		{`{"requests":[{"putKv":{"ledger":"a","key":"k","value":"AQID"}},{"deleteKv":{"ledger":"a","key":"k"}}]}`, true},
		{`{"request":{"requests":[{"createQueryCheckpoint":{}}]},"payload":"AQ=="}`, true},
		{`{"requests":[]}`, false},
		{`{"requests":[{}]}`, false},
		{`{"requests":[{"createLedger":{},"deleteLedger":{}}]}`, false},
		{`{"requests":[{"create_ledger":{}}]}`, false},
		{`{"requests":[{"createLedger":{"name":null}}]}`, false},
		{`{"requests":[{"createLedger":{}}],"idempotencyKey":""}`, false},
		{`{"requests":[{"apply":{"action":{"createTransaction":{},"revertTransaction":{}}}}]}`, false},
		{`{"requests":[{"saveLedgerMetadata":{"metadata":{"key":"untyped"}}}]}`, false},
	} {
		t.Run(fixture.body, func(t *testing.T) {
			var value any
			decoder := json.NewDecoder(strings.NewReader(fixture.body))
			decoder.UseNumber()
			require.NoError(t, decoder.Decode(&value))
			err := request.VisitJSON(value)
			if fixture.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
