package http

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
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

// TestProtojsonRoutes_TableIsComplete keeps protojsonRoutes honest. The table
// above is hand-written, so a new protojson handler added without a row would
// escape the guard entirely. Parse this package and assert the number of
// protojson call sites matches the number of rows.
//
// Counting is deliberate rather than name-matching: a call site cannot be mapped
// back to a route path statically without much heavier analysis, and a count
// mismatch is enough to force the author to look at the table.
func TestProtojsonRoutes_TableIsComplete(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	require.NoError(t, err)

	var sites []string

	for _, pkg := range pkgs {
		for path, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}

				name := calleeName(call.Fun)
				if name == "writeProtoOK" || name == "writeProtoListOK" || name == "protojson.Marshal" {
					sites = append(sites, filepath.Base(path)+": "+name)
				}

				return true
			})
		}
	}

	// response.go defines writeProtoOK and writeProtoListOK, each containing one
	// protojson.Marshal call. Those two are implementations, not routes.
	const implementationSites = 2

	require.Lenf(t, sites, len(protojsonRoutes)+implementationSites,
		"protojson call sites (%v) do not match protojsonRoutes (%d rows) + %d implementation sites. "+
			"If you added a protojson handler, add it to protojsonRoutes. If you removed one, delete its row.",
		sites, len(protojsonRoutes), implementationSites)
}

// calleeName renders a call's function as "name" or "pkg.name".
func calleeName(fun ast.Expr) string {
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.IndexExpr: // Generic instantiation, e.g. writeProtoListOK[T].
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
