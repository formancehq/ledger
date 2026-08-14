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
)

// TestSonicRoutes_PayloadHasCustomMarshalJSON guards the three routes fixed by
// EN-1622, which rely on their type's marshaller being the contract. If a
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

// allowedProtojsonImporters are the only files in this package permitted to
// import protojson. After EN-1791 exactly one remains, and it is DECODE-side:
// handlers_create_ledger.go uses protojson.Unmarshal to parse a
// MirrorRewriteRule oneof out of an incoming request body, which sonic's
// decoder cannot dispatch.
//
// This gate exists because matching call SHAPES is not sound: protojson is
// reachable as protojson.Marshal(m), as protojson.MarshalOptions{...}.Marshal(m)
// (the idiom used in cmd/ledgerctl), through a variable, or under an import
// alias. An import cannot be disguised, so gating it catches every form.
//
// The per-file granularity used to be a documented weakness: an allowlisted
// file could add a response-side protojson call and slip past both checks. That
// no longer applies, because no allowlisted file marshals a response at all —
// the one entry here only decodes. A new entry is a review checkpoint: state
// whether the file decodes a REQUEST (acceptable) or marshals a RESPONSE (not
// acceptable — write a DTO instead, see dto_indexes.go).
var allowedProtojsonImporters = map[string]bool{
	"handlers_create_ledger.go": true,
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
				"%s imports protojson directly. No response body in this package may be "+
					"serialized by protojson: write a hand-rolled DTO that owns the wire "+
					"shape instead (see dto_indexes.go). If %s only DECODES a request body "+
					"with protojson.Unmarshal, add it to allowedProtojsonImporters with a "+
					"comment saying so.",
				name, name)
		}
	}
}

// TestNoProtojsonMarshalInResponsePath is the primary enforcement: no response
// body in this package is serialized by protojson.
//
// protojson works off protobuf reflection and ignores json.Marshaler, so a type
// that has BOTH is silently rendered by the wrong one. That was the EN-1622
// defect: Transaction, Chapter and Log each had a hand-written MarshalJSON that
// the protojson writers bypassed, so the list routes disagreed with their
// sibling detail routes and leaked {v0}/{data} proto wrappers. EN-1791 removed
// the last response-side protojson call sites by giving every affected route a
// hand-written DTO, and deleted writeProtoOK / writeProtoListOK along with
// them. This test asserts that state does not regress.
//
// The two deleted writer names are still matched, deliberately: re-adding
// either one is exactly the regression this guards, and matching the name
// catches it before it grows call sites.
//
// handlers_create_ledger.go does not trip this test even though it imports
// protojson: the detector matches protojson.Marshal, and that file calls
// protojson.Unmarshal on an incoming request body. Decoding a request is not a
// response-path concern (see allowedProtojsonImporters).
func TestNoProtojsonMarshalInResponsePath(t *testing.T) {
	t.Parallel()

	// parser.ParseDir is deprecated as of Go 1.25 (SA1019); walk the directory
	// and parse each non-test file individually instead of grouping by package.
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()

	var sites []string // "filename: calleeName", for the diagnosable failure message.

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
			}

			return true
		})
	}

	require.Emptyf(t, sites,
		"found response-side proto serialization call sites %v. This package must not "+
			"serialize a response body through protojson, and writeProtoOK / "+
			"writeProtoListOK were deleted by EN-1791 rather than kept: protojson emits "+
			"quoted uint64, {\"data\":<micros>} timestamps and drops meaningful zeros, and "+
			"it ignores a type's own MarshalJSON. Write a hand-rolled DTO that owns the "+
			"wire shape instead — see dto_indexes.go — and type the response in "+
			"openapi.yml. Note the blast radius before reaching for a MarshalJSON on the "+
			"proto type itself: the CLI (cmd/ledgerctl/cmdutil/output.go) also prefers a "+
			"custom MarshalJSON when one exists, and misc/operator parses "+
			"`ledgerctl indexes list --json`. See "+
			"docs/technical/architecture/subsystems/api/http-api.md for the rule.",
		sites)
}

// responsePayloads are the EN-1791 response DTOs, which render their own fields
// rather than delegating to a proto marshaller.
//
// This is a DECLARED TABLE, not a package-wide law. A blanket ban on
// proto.Message inside any response payload would be wrong:
// numscriptVersionsDTO legitimately embeds a *commonpb.Timestamp and MUST NOT
// be added here — sonic invokes that type's MarshalJSON, which emits
// RFC3339Nano, so the shape is already correct and rewriting it would churn an
// unrelated, non-defective DTO.
//
// The EN-1791 types follow the convention for two concrete reasons.
// commonpb.Timestamp.MarshalJSON is also consumed by the CLI and, transitively,
// by misc/operator, so depending on it from the HTTP wire would tie this
// surface to a cross-module contract that a root `go build ./...` never even
// compiles. And a plain RFC3339Nano string maps directly onto OpenAPI's
// `type: string, format: date-time`, whereas a proto type's rendering has to be
// described second-hand.
var responsePayloads = []any{
	indexDTO{},
	indexEntryDTO{},
	indexStatusDTO{},
	signingKeyDTO{},
	eventsSinksDTO{},
}

// TestResponsePayloadsContainNoProtoMessage walks each declared payload
// recursively and fails if any field's type — or a pointer to it — implements
// proto.Message. See responsePayloads for the scope of the rule.
func TestResponsePayloadsContainNoProtoMessage(t *testing.T) {
	t.Parallel()

	protoMessageType := reflect.TypeFor[proto.Message]()

	for _, payload := range responsePayloads {
		root := reflect.TypeOf(payload)

		t.Run(root.Name(), func(t *testing.T) {
			t.Parallel()

			// Guards against a cycle through a self-referential pointer field, and
			// against re-walking a type shared by several fields.
			visited := map[reflect.Type]bool{}

			var walk func(typ reflect.Type, path string)
			walk = func(typ reflect.Type, path string) {
				if visited[typ] {
					return
				}

				visited[typ] = true

				for field := range typ.Fields() {
					fieldPath := path + "." + field.Name

					require.Falsef(t,
						field.Type.Implements(protoMessageType) ||
							reflect.PointerTo(field.Type).Implements(protoMessageType),
						"%s is a proto.Message (%s). This DTO renders its own fields so this "+
							"package owns the wire shape; embedding a proto type delegates the "+
							"rendering to that type's MarshalJSON, which the CLI and "+
							"misc/operator also consume. Convert the value instead — see "+
							"formatTimestamp in dto_indexes.go.",
						fieldPath, field.Type)

					// Deref through pointer/slice/array to reach the element type a
					// proto field would hide behind.
					elem := field.Type
					for elem.Kind() == reflect.Pointer ||
						elem.Kind() == reflect.Slice ||
						elem.Kind() == reflect.Array {
						elem = elem.Elem()

						require.Falsef(t,
							elem.Implements(protoMessageType) ||
								reflect.PointerTo(elem).Implements(protoMessageType),
							"%s holds a proto.Message (%s). This DTO renders its own fields so "+
								"this package owns the wire shape; embedding a proto type "+
								"delegates the rendering to that type's MarshalJSON, which the CLI "+
								"and misc/operator also consume. Convert the value instead — see "+
								"formatTimestamp in dto_indexes.go.",
							fieldPath, elem)
					}

					if elem.Kind() == reflect.Struct {
						walk(elem, fieldPath)
					}
				}
			}

			walk(root, root.Name())
		})
	}
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
