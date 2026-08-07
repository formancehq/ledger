package internal

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type assertionUse struct {
	function string
	path     string
}

func TestConditionalSafetyPropertiesDoNotRequireCoverage(t *testing.T) {
	t.Parallel()

	want := map[string]string{
		"a reference never maps to more than one committed transaction":  "AlwaysOrUnreachable",
		"committed sentinel transaction must survive operational events": "Unreachable",
		"definitively rejected write never appears in the ledger":        "AlwaysOrUnreachable",
		"failed atomic bulk leaves no partial account activity":          "AlwaysOrUnreachable",
		"failed atomic bulk leaves no partial transaction effects":       "AlwaysOrUnreachable",
		"list/get balance divergence persisted past quiescence":          "Unreachable",
		"predecessor references are reusable after ledger recreate":      "AlwaysOrUnreachable",
		"recreated ledger never exposes predecessor account activity":    "AlwaysOrUnreachable",
		"recreated ledger never exposes predecessor transactions":        "AlwaysOrUnreachable",
	}
	found := make(map[string][]assertionUse, len(want))
	workloadFS := os.DirFS("..")

	err := fs.WalkDir(workloadFS, ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}

		source, err := fs.ReadFile(workloadFS, path)
		if err != nil {
			return err
		}
		return collectAssertionUses(path, source, found)
	})
	if err != nil {
		t.Fatalf("scan workload assertions: %v", err)
	}

	serverFiles := []string{
		"internal/infra/node/applier.go",
		"internal/infra/state/cache_snapshotter.go",
		"internal/infra/state/machine.go",
		"internal/infra/state/write_set.go",
		"internal/storage/wal/wal_default.go",
	}
	for _, path := range serverFiles {
		source, err := os.ReadFile(filepath.Join("../../../..", path))
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if err := collectAssertionUses(path, source, found); err != nil {
			t.Fatalf("scan %s: %v", path, err)
		}
	}

	for message, expectedFunction := range want {
		uses := found[message]
		if len(uses) == 0 {
			t.Errorf("assertion %q was not found", message)
			continue
		}
		for _, use := range uses {
			if use.function != expectedFunction {
				t.Errorf("assertion %q in %s uses assert.%s; want assert.%s", message, use.path, use.function, expectedFunction)
			}
		}
	}

	optionalDiagnostics := []string{
		"concurrent ledger delete: delete failed transiently",
		"concurrent ledger delete: post-delete write inconclusive (transient)",
		"config patch applied",
		"follower observed same ClusterConfig as leader",
		"rolling-restart deleted pod",
		"sentinel verify hit a transient error",
		"STS ready after config patch",
		"old-term entry committed and resolved in same batch as a sweep",
		"predecessor reference accepted by recreated ledger",
		"bloom SetReady skipped: rebuild raced populate",
		"deleted ledger deferred cleanup executed by covering purge",
		"snap file recovered from WAL snapshot records",
		"stale proposal rejected: cache epoch mismatch",
		"stale proposal rejected: predicted index mismatch",
	}
	for _, message := range optionalDiagnostics {
		for _, use := range found[message] {
			t.Errorf("optional diagnostic %q in %s uses assert.%s; use lifecycle.SendEvent instead", message, use.path, use.function)
		}
	}

	barrierUses := found["barrier succeeded after an observed leadership change"]
	if len(barrierUses) != 1 {
		t.Errorf("leadership-change barrier assertion has %d call sites; want exactly one", len(barrierUses))
	} else if use := barrierUses[0]; use.function != "Sometimes" || !strings.HasSuffix(use.path, "parallel_driver_transfer_leadership/main.go") {
		t.Errorf("leadership-change barrier assertion uses assert.%s in %s; want assert.Sometimes in parallel_driver_transfer_leadership", use.function, use.path)
	}
}

func collectAssertionUses(path string, source []byte, found map[string][]assertionUse) error {
	file, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
	if err != nil {
		return err
	}

	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok || pkg.Name != "assert" {
			return true
		}

		messageIndex := 1
		if selector.Sel.Name == "Reachable" || selector.Sel.Name == "Unreachable" {
			messageIndex = 0
		}
		if len(call.Args) <= messageIndex {
			return true
		}
		literal, ok := call.Args[messageIndex].(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		message, err := strconv.Unquote(literal.Value)
		if err != nil {
			return true
		}
		found[message] = append(found[message], assertionUse{function: selector.Sel.Name, path: path})

		return true
	})

	return nil
}
