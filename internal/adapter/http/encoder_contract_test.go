package http

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"sort"
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

// TestSonicRoutes_PayloadHasCustomMarshalJSON is the converse: every payload
// served through one of the sonic writers relies on its type's marshaller being
// the contract. If a marshaller is ever deleted, sonic falls back to the
// protoc-gen `json:` tags, which are snake_case — a silent wire regression with
// no compile error. The handler tests assert the resulting shape; this asserts
// the mechanism they depend on.
//
// Transaction, Chapter and Log are the three EN-1622 routes. The other three
// arrived with EN-1779's opt-in string amounts, which routed them through the
// checked writers as well: PreparedQueryCursor on the prepared-query execute
// route (an exposure that predates EN-1779 — the cursor already carried its own
// marshaller), CreatedTransaction on create-transaction and RevertedTransaction
// on revert-transaction. Their opt-in wrappers delegate to the same buildAux
// the marshaller uses, so deleting a marshaller breaks BOTH modes at once.
func TestSonicRoutes_PayloadHasCustomMarshalJSON(t *testing.T) {
	t.Parallel()

	for _, msg := range []proto.Message{
		&commonpb.Transaction{},
		&commonpb.Chapter{},
		&commonpb.Log{},
		&commonpb.PreparedQueryCursor{},
		&commonpb.CreatedTransaction{},
		&commonpb.RevertedTransaction{},
	} {
		t.Run(reflect.TypeOf(msg).Elem().Name(), func(t *testing.T) {
			t.Parallel()

			_, hasCustom := msg.(json.Marshaler)
			require.Truef(t, hasCustom,
				"%T is served through a checked sonic writer and MUST keep its custom "+
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

// responseWriterCallSites pins which JSON response writer each file in this
// package calls. The mapping is a wire contract, not a style choice: this
// package writes JSON through two different sonic configurations, and the
// writer a route calls decides which one it gets.
//
//	writer               encoder                        a `&` in the body   trailing newline
//	-------------------- ------------------------------ ------------------- ----------------
//	writeJSONResponse    json.MarshalWrite, ConfigStd   escaped             appended
//	writeOK              via writeJSONResponse          escaped             appended
//	writeCreated         via writeJSONResponse          escaped             appended
//	writeOKChecked       json.Marshal, ConfigDefault    literal             absent
//	writeCheckedBody     json.MarshalWrite, ConfigStd   escaped             appended
//	writeCheckedStatus   via writeCheckedBody           escaped             appended
//
// So swapping a route from writeOKChecked to writeCheckedStatus — or the other
// way — changes every response body that route ever emits: a literal `&`
// becomes the six-byte escape \u0026, and a newline appears (or disappears)
// at the end. Nothing in the type
// system stops that swap, and no handler test catches it unless its pinned body
// happens to contain an HTML-sensitive character. This table is the guard.
//
// The checked writers exist for two independent reasons, which is why a route
// cannot pick one freely:
//   - error routing: a payload whose MarshalJSON can genuinely fail must be
//     buffered before the header is written, so the failure becomes a clean 500
//     instead of an error object appended to a committed 200 (invariant #7);
//   - encoder preservation: writeCheckedStatus is the ConfigStd twin of
//     writeOKChecked, added by EN-1779 so a route that gained the opt-in
//     string-amount branch could become buffered WITHOUT changing encoder.
//
// A row lists the DISTINCT writers a file calls, not one entry per call site:
// two branches of the same handler calling the same writer is normal (the
// EN-1779 opt-in branch does exactly that), while a branch calling a different
// writer than its sibling is the bug this pins.
var responseWriterCallSites = []struct {
	file    string
	writers []string
}{
	{"handlers_aggregate_volumes.go", []string{"writeOK"}},
	{"handlers_analyze_accounts.go", []string{"writeOK"}},
	{"handlers_analyze_transactions.go", []string{"writeOK"}},
	// The bulk route builds its own top-level envelope, so the success path
	// takes writeCheckedBody rather than writeCheckedStatus; the error path
	// still streams through writeJSONResponse.
	{"handlers_bulk.go", []string{"writeCheckedBody", "writeJSONResponse"}},
	{"handlers_create_index.go", []string{"writeCreated"}},
	{"handlers_create_ledger.go", []string{"writeCreated"}},
	// Both transaction-creating routes pair writeCreated (default wire) with
	// writeCheckedStatus (opt-in wire). That pairing is the EN-1779 invariant:
	// writeCheckedStatus shares writeCreated's ConfigStd encoder, so the two
	// branches differ only in the amount's quoting.
	{"handlers_create_transaction.go", []string{"writeCreated", "writeCheckedStatus"}},
	{"handlers_execute_prepared_query.go", []string{"writeCheckedBody"}},
	{"handlers_get_account.go", []string{"writeOK"}},
	{"handlers_get_account_type.go", []string{"writeOK"}},
	{"handlers_get_audit_entry.go", []string{"writeOKChecked"}},
	{"handlers_get_chapter_schedule.go", []string{"writeOK"}},
	{"handlers_get_events_sinks.go", []string{"writeOK"}},
	{"handlers_get_ledger.go", []string{"writeOK"}},
	{"handlers_get_ledger_stats.go", []string{"writeOK"}},
	{"handlers_get_log.go", []string{"writeOKChecked"}},
	{"handlers_get_metadata_schema.go", []string{"writeOK"}},
	{"handlers_get_numscript.go", []string{"writeOK"}},
	{"handlers_get_numscript_usage.go", []string{"writeOK"}},
	{"handlers_get_transaction.go", []string{"writeCheckedStatus"}},
	{"handlers_health.go", []string{"writeOK"}},
	{"handlers_inspect_index.go", []string{"writeOK"}},
	{"handlers_list_account_types.go", []string{"writeOK"}},
	{"handlers_list_accounts.go", []string{"writeOK"}},
	{"handlers_list_all_ledgers.go", []string{"writeOK"}},
	{"handlers_list_audit_entries.go", []string{"writeOKChecked"}},
	{"handlers_list_chapters.go", []string{"writeOKChecked"}},
	{"handlers_list_ledger_logs.go", []string{"writeCheckedStatus"}},
	{"handlers_list_numscript_versions.go", []string{"writeOK"}},
	{"handlers_list_numscripts.go", []string{"writeOK"}},
	{"handlers_list_prepared_queries.go", []string{"writeOK"}},
	{"handlers_list_transactions.go", []string{"writeOKChecked"}},
	{"handlers_promote_ledger.go", []string{"writeCreated"}},
	{"handlers_revert_transaction.go", []string{"writeCreated", "writeCheckedStatus"}},
	{"handlers_save_numscript.go", []string{"writeCreated"}},
	// response.go is not a route: these are the internal delegations that give
	// the table above its meaning — writeOK and writeCreated forward to
	// writeJSONResponse, writeCheckedStatus forwards to writeCheckedBody, and
	// writeProtoOK/writeProtoListOK forward to writeOK. They are listed for the
	// same reason TestProtojsonRoutes_TableIsComplete lists response.go twice:
	// the AST sees them, so the table must too.
	{"response.go", []string{"writeCheckedBody", "writeJSONResponse", "writeOK"}},
}

// responseWriterNames is the set of writers the AST scan below recognises.
// Every function that writes a JSON body with a status code belongs here; a
// new one that is not listed would be invisible to the guard.
var responseWriterNames = map[string]bool{
	"writeJSONResponse":  true,
	"writeOK":            true,
	"writeCreated":       true,
	"writeOKChecked":     true,
	"writeCheckedBody":   true,
	"writeCheckedStatus": true,
}

// TestResponseWriters_TableIsComplete pins the file-to-writer mapping above by
// AST-scanning the package, exactly as TestProtojsonRoutes_TableIsComplete pins
// the protojson call sites. It compares SETS of "file: writer" pairs, and the
// pair — not just the file — is what makes it useful: a route that keeps its
// file but changes writer changes encoder, so it must show up as one missing
// pair and one unexpected pair rather than cancelling out.
//
// A count would not do. Moving a route from writeOKChecked to
// writeCheckedStatus leaves the number of writer call sites unchanged, and so
// does any compensating add/remove pair across two files.
func TestResponseWriters_TableIsComplete(t *testing.T) {
	t.Parallel()

	// parser.ParseDir is deprecated as of Go 1.25 (SA1019); walk the directory
	// and parse each non-test file individually instead of grouping by package.
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	seen := map[string]bool{}

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

			if name := calleeName(call.Fun); responseWriterNames[name] {
				seen[filename+": "+name] = true
			}

			return true
		})
	}

	expected := map[string]bool{}

	for _, row := range responseWriterCallSites {
		for _, writer := range row.writers {
			expected[row.file+": "+writer] = true
		}
	}

	require.ElementsMatch(t, sortedKeys(seen), sortedKeys(expected),
		"the JSON writers this package calls do not match responseWriterCallSites: "+
			"list A above is what the AST found, list B is what the table declares.\n\n"+
			"If you added a route, add a row naming the writer the route ACTUALLY "+
			"calls — a row naming a different writer satisfies this check while "+
			"guarding nothing. If you removed a route, delete its row. If a row's "+
			"writer changed, that is a WIRE CHANGE: the two encoders differ in HTML "+
			"escaping and in the trailing newline, so update the row deliberately and "+
			"re-pin every affected body in the handler test. If you changed neither, "+
			"calleeName probably did not recognise your call shape, or the writer is "+
			"missing from responseWriterNames — fix the scan, do not delete a row.")
}

// sortedKeys returns a set's keys in a stable order, so a failure message reads
// the same on every run.
func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}

	sort.Strings(out)

	return out
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
