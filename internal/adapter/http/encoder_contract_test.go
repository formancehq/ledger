package http

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
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
	file  string // Handler file containing this route's protojson call site.
	msg   proto.Message
}{
	{"GET /v3/{ledgerName}/indexes", "handlers_list_ledger_indexes.go", &commonpb.Index{}},
	{"GET /v3/{ledgerName}/indexes/{canonicalId}", "handlers_get_index.go", &commonpb.Index{}},
	{"GET /v3/_/indexes", "handlers_list_bucket_indexes.go", &commonpb.Index{}},
	{"GET /v3/_/indexes/{canonicalId}", "handlers_get_bucket_index.go", &commonpb.Index{}},
	{"GET /v3/{ledgerName}/indexes/{canonicalId}/status", "handlers_get_index_entry_status.go", &servicepb.IndexEntry{}},
	{"GET /v3/_/indexes/{canonicalId}/status", "handlers_get_bucket_index_entry_status.go", &servicepb.IndexEntry{}},
	{"GET /v3/_/indexes/status", "handlers_get_index_status.go", &servicepb.GetIndexStatusResponse{}},
	{"GET /v3/_/signing-keys", "handlers_list_signing_keys.go", &commonpb.SigningKey{}},
	{"GET /v3/_/events-sinks", "handlers_get_events_sinks.go", &servicepb.GetEventsSinksResponse{}},
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

// TestSonicRoutes_PayloadHasCustomMarshalJSON is the converse: the routes
// fixed by EN-1622 rely on their type's marshaller being the contract. If a
// marshaller is ever deleted, sonic falls back to the protoc-gen `json:` tags,
// which are snake_case — a silent wire regression. The handler tests assert the
// resulting shape; this asserts the mechanism they depend on.
func TestSonicRoutes_PayloadHasCustomMarshalJSON(t *testing.T) {
	t.Parallel()

	for _, msg := range []proto.Message{
		&commonpb.Transaction{},
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

// allowedProtojsonImporters are the only files in this package permitted to
// import protojson directly. response.go implements writeProtoOK and
// writeProtoListOK; handlers_get_events_sinks.go predates those helpers and
// calls protojson inline; handlers_create_ledger.go is the odd one out — it
// uses protojson.Unmarshal (decode-side) to parse a MirrorRewriteRule oneof
// out of an incoming request body, which sonic's decoder cannot dispatch.
// That file never marshals a response through protojson, so it carries no
// protojsonRoutes row, but the gate below fires on the import itself, so it
// still needs an entry here.
//
// This gate exists because matching call SHAPES is not sound: protojson is
// reachable as protojson.Marshal(m), as protojson.MarshalOptions{...}.Marshal(m)
// (the idiom used in cmd/ledgerctl), through a variable, or under an import
// alias. An import cannot be disguised, so gating it catches every form.
//
// KNOWN LIMIT: this allowlist is per-FILE, so an allowlisted file could later
// add a response-side protojson call and slip past both checks — the import
// gate because the file is exempt, and TestProtojsonRoutes_TableIsComplete
// because calleeName cannot see a protojson.MarshalOptions{...}.Marshal(...)
// receiver either. Closing that gap would require symbol-level analysis, which
// reopens the call-shape unsoundness this gate replaced, so it is accepted
// rather than closed. Keep the allowlist minimal, justify every entry, and
// treat a new entry as a review checkpoint: state whether the file marshals a
// RESPONSE (needs a protojsonRoutes row) or only decodes a REQUEST (does not).
var allowedProtojsonImporters = map[string]bool{
	"response.go":                  true,
	"handlers_get_events_sinks.go": true,
	"handlers_create_ledger.go":    true,
}

func TestProtojsonImportIsRestricted(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(fset, name, nil, parser.ImportsOnly)
		require.NoError(t, err)

		for _, imp := range file.Imports {
			if imp.Path.Value != `"google.golang.org/protobuf/encoding/protojson"` {
				continue
			}

			require.Truef(t, allowedProtojsonImporters[name],
				"%s imports protojson directly. Serialize through writeProtoOK or "+
					"writeProtoListOK and add the route to protojsonRoutes with the exact "+
					"message type the handler writes. If the handler genuinely must call "+
					"protojson itself, add %s to allowedProtojsonImporters AND add its "+
					"protojsonRoutes row — do not do only one.",
				name, name)
		}
	}
}

// TestProtojsonRoutes_TableIsComplete keeps protojsonRoutes honest. The table
// above is hand-written, so a new protojson handler added without a row would
// escape the guard entirely. Parse this package and compare the SET of files
// containing a protojson call site against the set of files the table expects.
//
// A bare count is not enough: a compensating add/remove pair (one handler
// added, one removed) leaves the total unchanged, so a stale row plus an
// unguarded new route would both pass a count check. Comparing sets keyed by
// filename catches that, since the added file is present but unexpected and
// the removed file is expected but absent. A call site still cannot be mapped
// back to a specific route path without much heavier analysis, which is why
// the table's `file` field — not `route` — is what this test checks against.
func TestProtojsonRoutes_TableIsComplete(t *testing.T) {
	t.Parallel()

	// parser.ParseDir is deprecated as of Go 1.25 (SA1019); walk the directory
	// and parse each non-test file individually instead of grouping by package.
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()

	var sites []string     // "filename: calleeName", for the diagnosable failure message.
	var siteFiles []string // filename only, for the set comparison below.

	for _, entry := range entries {
		filename := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(filename, ".go") || strings.HasSuffix(filename, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(fset, filename, nil, 0)
		require.NoError(t, err)

		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}

			name := calleeName(call.Fun)
			if name == "writeProtoOK" || name == "writeProtoListOK" || name == "protojson.Marshal" {
				sites = append(sites, filename+": "+name)
				siteFiles = append(siteFiles, filename)
			}

			return true
		})
	}

	// Built from the table so it cannot drift: one entry per route row, plus
	// response.go twice for the two implementation call sites inside
	// writeProtoOK and writeProtoListOK themselves — those two functions are
	// what every route row calls through, not routes of their own.
	expected := make([]string, 0, len(protojsonRoutes)+2)
	for _, tc := range protojsonRoutes {
		expected = append(expected, tc.file)
	}

	expected = append(expected, "response.go", "response.go")

	require.ElementsMatchf(t, siteFiles, expected,
		"protojson call sites %v do not match the expected set %v. If you added a "+
			"protojson handler, add a row with the exact message type the handler "+
			"writes — a placeholder type satisfies this check while guarding nothing. "+
			"If you removed a handler, delete its row. If you added neither, calleeName "+
			"probably did not recognise your call shape — fix the parser, do not delete "+
			"a row.",
		sites, expected)
}

// calleeName renders a call's function as "name" or "pkg.name".
func calleeName(fun ast.Expr) string {
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.IndexExpr: // Generic instantiation, e.g. writeProtoListOK[T].
		return calleeName(f.X)
	case *ast.IndexListExpr: // Generic instantiation with multiple type parameters.
		return calleeName(f.X)
	case *ast.SelectorExpr:
		if x, ok := f.X.(*ast.Ident); ok {
			return x.Name + "." + f.Sel.Name
		}

		return f.Sel.Name
	default:
		return ""
	}
}
