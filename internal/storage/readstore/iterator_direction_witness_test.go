package readstore

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIteratorMethodSetNamesDirection guards the property that makes
// Iterator[Asc] and Iterator[Desc] DIFFERENT types.
//
// Go decides interface satisfaction structurally. While D appeared in no
// method signature, the two instantiations had identical method sets and were
// freely interchangeable: `PaginateReverse(NewSliceIterator(nil), 10, nil)`
// compiled and would have returned a page in ascending order through a
// descending cursor. Every ordinary `var _ EntityIterator = (*T)(nil)`
// assertion in this package compiled just as happily before the fix as after,
// which is why none of them can serve as the guard.
//
// This is therefore a source-level assertion: it fails if a future edit drops
// D from Iterator's method set, restoring the interchangeability. It is not a
// substitute for the per-implementation assertions — those pin that each
// concrete type declares the direction it actually walks.
func TestIteratorMethodSetNamesDirection(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, "iterator.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err, "parsing iterator.go")

	iface := findInterface(t, file, "Iterator")

	var mentioning []string

	for _, field := range iface.Methods.List {
		fn, ok := field.Type.(*ast.FuncType)
		if !ok {
			continue
		}

		if signatureMentions(fn, "D") {
			mentioning = append(mentioning, field.Names[0].Name)
		}
	}

	require.NotEmpty(t, mentioning,
		"Iterator's type parameter D appears in no method signature, so Iterator[Asc] "+
			"and Iterator[Desc] have identical method sets and satisfy each other. "+
			"An ascending iterator can then be passed to PaginateReverse. Keep a method "+
			"whose signature names D (see Iterator.Direction).")
}

// findInterface returns the named interface type declared in file.
func findInterface(t *testing.T, file *ast.File, name string) *ast.InterfaceType {
	t.Helper()

	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}

		for _, spec := range gen.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok || ts.Name.Name != name {
				continue
			}

			iface, ok := ts.Type.(*ast.InterfaceType)
			require.True(t, ok, "%s is not an interface", name)

			return iface
		}
	}

	t.Fatalf("interface %s not found in iterator.go", name)

	return nil
}

// signatureMentions reports whether ident appears anywhere in fn's parameter
// or result types.
func signatureMentions(fn *ast.FuncType, ident string) bool {
	found := false

	inspect := func(fields *ast.FieldList) {
		if fields == nil {
			return
		}

		for _, f := range fields.List {
			ast.Inspect(f.Type, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && id.Name == ident {
					found = true
				}

				return !found
			})
		}
	}

	inspect(fn.Params)
	inspect(fn.Results)

	return found
}
