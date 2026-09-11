package grpc

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// EN-1954: BucketService.Apply is the only gRPC entry point for an audited
// business write.
//
// Five convenience RPCs (CreatePreparedQuery, UpdatePreparedQuery,
// DeletePreparedQuery, CreateQueryCheckpoint, DeleteQueryCheckpoint) each
// rebuilt a one-request unsigned ApplyRequest and entered admission on their
// own. That second path was unbatchable, unsignable, and carried its own
// authentication and forwarding logic — which is how EN-1950 lost the
// authenticated caller context on a write that still committed an audit entry.
//
// These two tests fail when a new handler recreates that shape.

// applyWriteSites lists every call site in this package allowed to enter the
// audited write path, keyed by "file.go:Receiver.Function".
//
// If this test fails because you added a mutation RPC, do NOT add a row. The
// operation belongs in ledger.Request and travels through Apply, where it is
// batchable, idempotent, signable, scope-checked once, and forwarded once.
// Add a row only for a call that is provably not an audited business write,
// and say why in the value.
var applyWriteSites = map[string]string{
	"server_bucket.go:BucketServiceServerImpl.Apply": "the single gRPC business-write entry point",
	"client_bucket.go:BucketGrpcClient.Apply":        "the follower-side forwarder relaying that same batch to the leader",
}

// writePathCalls are the calls that put a business mutation on the Raft log, or
// build the envelope that carries one.
var writePathCalls = map[string]bool{
	"Apply":                true, // ctrl.Controller.Apply / BucketServiceClient.Apply
	"Admit":                true, // ctrl.Admission.Admit
	"UnsignedApplyRequest": true,
	"SignedApplyRequest":   true,
}

func TestApplyIsTheOnlyGRPCWriteEntryPoint(t *testing.T) {
	t.Parallel()

	var found []string

	forEachPackageFile(t, func(path string, file *ast.File) {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}

			site := filepath.Base(path) + ":" + funcIdentity(fn)

			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}

				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || !writePathCalls[sel.Sel.Name] {
					return true
				}

				if _, allowed := applyWriteSites[site]; !allowed {
					found = append(found, site+" calls "+sel.Sel.Name)
				}

				return true
			})
		}
	})

	sort.Strings(found)
	require.Empty(t, found,
		"a gRPC handler outside BucketService.Apply entered the audited write path; "+
			"move the operation into ledger.Request and submit it through Apply")
}

// TestClusterServiceHasNoAdmissionDependency keeps the removal structural
// rather than conventional: without the field, a future ClusterService handler
// cannot admit a proposal even by accident.
func TestClusterServiceHasNoAdmissionDependency(t *testing.T) {
	t.Parallel()

	var found []string

	forEachPackageFile(t, func(path string, file *ast.File) {
		ast.Inspect(file, func(n ast.Node) bool {
			spec, ok := n.(*ast.TypeSpec)
			if !ok {
				return true
			}

			structType, ok := spec.Type.(*ast.StructType)
			if !ok || spec.Name.Name != "ClusterServiceServerImpl" {
				return true
			}

			for _, field := range structType.Fields.List {
				sel, ok := field.Type.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Admission" {
					continue
				}

				found = append(found, filepath.Base(path)+":"+spec.Name.Name)
			}

			return true
		})
	})

	require.Empty(t, found,
		"ClusterServiceServerImpl must not depend on ctrl.Admission; "+
			"audited writes belong to BucketService.Apply")
}

// forEachPackageFile parses this package's production sources. Test files are
// excluded: they legitimately drive Apply and Admit directly.
func forEachPackageFile(t *testing.T, visit func(path string, file *ast.File)) {
	t.Helper()

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fileSet := token.NewFileSet()

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(fileSet, name, nil, parser.SkipObjectResolution)
		require.NoErrorf(t, err, "parsing %s", name)

		visit(name, file)
	}
}

// funcIdentity renders "Receiver.Method" for a method and "Function" otherwise.
func funcIdentity(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}

	recv := fn.Recv.List[0].Type
	if star, ok := recv.(*ast.StarExpr); ok {
		recv = star.X
	}

	ident, ok := recv.(*ast.Ident)
	if !ok {
		return fn.Name.Name
	}

	return ident.Name + "." + fn.Name.Name
}
