package http

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// protojsonRoutes lists every HTTP route whose 200 body is serialized by
// protojson — writeProtoOK, writeProtoListOK, or an inline protojson.Marshal.
//
// protojson works off protobuf reflection and ignores json.Marshaler, so a type
// that has BOTH is silently rendered by the wrong one. That is the EN-1622
// defect: Transaction, Chapter and Log each had a hand-written MarshalJSON that
// the protojson writers bypassed, so the list routes disagreed with their
// sibling detail routes and leaked {v0}/{data} proto wrappers.
//
// If this test fails because you added a MarshalJSON to one of these types, do
// NOT delete the row. The marshaller is the public contract once it exists:
// move the handler to writeOKChecked, add wire assertions to its handler test,
// and type the response in openapi.yml. Note the blast radius first — the CLI
// (cmd/ledgerctl/cmdutil/output.go) also prefers a custom MarshalJSON when one
// exists, and misc/operator parses `ledgerctl indexes list --json`.
//
// See docs/technical/architecture/subsystems/api/http-api.md for the rule.
var protojsonRoutes = []struct {
	route string
	msg   proto.Message
}{
	{"GET /v3/{ledgerName}/indexes", &commonpb.Index{}},
	{"GET /v3/{ledgerName}/indexes/{canonicalId}", &commonpb.Index{}},
	{"GET /v3/_/indexes", &commonpb.Index{}},
	{"GET /v3/_/indexes/{canonicalId}", &commonpb.Index{}},
	{"GET /v3/{ledgerName}/indexes/{canonicalId}/status", &servicepb.IndexEntry{}},
	{"GET /v3/_/indexes/{canonicalId}/status", &servicepb.IndexEntry{}},
	{"GET /v3/_/indexes/status", &servicepb.GetIndexStatusResponse{}},
	{"GET /v3/_/signing-keys", &commonpb.SigningKey{}},
	{"GET /v3/_/events-sinks", &servicepb.GetEventsSinksResponse{}},
}

func TestProtojsonRoutes_PayloadHasNoCustomMarshalJSON(t *testing.T) {
	t.Parallel()

	for _, tc := range protojsonRoutes {
		t.Run(tc.route, func(t *testing.T) {
			t.Parallel()

			_, hasCustom := tc.msg.(json.Marshaler)
			require.Falsef(t, hasCustom,
				"%s serializes %T via protojson, but %T now implements json.Marshaler. "+
					"protojson ignores it, so the custom shape is silently discarded. "+
					"Move the handler to writeOKChecked and type the response in openapi.yml.",
				tc.route, tc.msg, tc.msg)
		})
	}
}

// TestSonicRoutes_PayloadHasCustomMarshalJSON is the converse: the three routes
// fixed by EN-1622 rely on their type's marshaller being the contract. If a
// marshaller is ever deleted, sonic falls back to the protoc-gen `json:` tags,
// which are snake_case — a silent wire regression. The handler tests assert the
// resulting shape; this asserts the mechanism they depend on.
func TestSonicRoutes_PayloadHasCustomMarshalJSON(t *testing.T) {
	t.Parallel()

	for _, msg := range []proto.Message{
		&commonpb.Transaction{},
		&commonpb.Chapter{},
		&commonpb.Log{},
	} {
		t.Run(reflect.TypeOf(msg).Elem().Name(), func(t *testing.T) {
			t.Parallel()

			_, hasCustom := msg.(json.Marshaler)
			require.Truef(t, hasCustom,
				"%T is served through writeOKChecked (sonic) and MUST keep its custom "+
					"MarshalJSON: without it sonic emits the protoc-gen snake_case tags.",
				msg)
		})
	}
}
