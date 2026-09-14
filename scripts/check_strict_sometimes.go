package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// run_model_test.sh treats a false `Sometimes` as normal, because a Sometimes
// is existential. Three assertions in the shared workload helpers are the
// exception: they are invariants written with the wrong primitive
// (`assert.Sometimes(IsTolerated(err), ...)` claims every call succeeds or
// fails transiently), and locally, with no fault injection and transients
// retried to a definitive outcome, they must hold on every call. The script
// keeps universal treatment for exactly those, listed in STRICT_SOMETIMES.
//
// A new tolerated-error probe added to the helpers and not to that list would
// silently lose its local strictness, so the two are pinned to each other.
const (
	strictSometimesScript = "tests/antithesis/run_model_test.sh"
	strictSometimesDir    = "tests/antithesis/workload/internal"
)

var strictSometimesDecl = regexp.MustCompile(`(?m)^STRICT_SOMETIMES='(\[.*\])'$`)

// checkStrictSometimes reports drift between the script's allowlist and the
// tolerated-error probes it is meant to name.
func checkStrictSometimes(files []string) ([]finding, error) {
	declared, err := strictSometimesAllowlist()
	if err != nil {
		return nil, err
	}

	actual, err := toleratedSometimesMessages(files)
	if err != nil {
		return nil, err
	}

	var findings []finding

	for _, msg := range missingFrom(actual, declared) {
		findings = append(findings, finding{
			path: strictSometimesScript, line: 1, column: 1,
			message: fmt.Sprintf("STRICT_SOMETIMES does not list the tolerated-error probe %q; "+
				"a false evaluation of it would not fail a local run", msg),
		})
	}

	for _, msg := range missingFrom(declared, actual) {
		findings = append(findings, finding{
			path: strictSometimesScript, line: 1, column: 1,
			message: fmt.Sprintf("STRICT_SOMETIMES lists %q, which no longer matches an "+
				"assert.Sometimes(IsTolerated(...)) call in "+strictSometimesDir, msg),
		})
	}

	return findings, nil
}

func missingFrom(want, have []string) []string {
	index := make(map[string]struct{}, len(have))
	for _, msg := range have {
		index[msg] = struct{}{}
	}

	var out []string

	for _, msg := range want {
		if _, ok := index[msg]; !ok {
			out = append(out, msg)
		}
	}

	return out
}

// strictSometimesAllowlist reads the JSON array the script declares.
func strictSometimesAllowlist() ([]string, error) {
	source, err := os.ReadFile(strictSometimesScript)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", strictSometimesScript, err)
	}

	return parseStrictSometimes(source)
}

// parseStrictSometimes extracts the allowlist from the script's source.
func parseStrictSometimes(source []byte) ([]string, error) {
	match := strictSometimesDecl.FindSubmatch(source)
	if match == nil {
		return nil, fmt.Errorf("%s: no STRICT_SOMETIMES declaration found", strictSometimesScript)
	}

	return parseJSONStringArray(string(match[1]))
}

// parseJSONStringArray decodes the flat ["a","b"] literal the script carries,
// without pulling encoding/json in for one line.
func parseJSONStringArray(literal string) ([]string, error) {
	trimmed := strings.TrimSpace(literal)
	if !strings.HasPrefix(trimmed, "[") || !strings.HasSuffix(trimmed, "]") {
		return nil, fmt.Errorf("STRICT_SOMETIMES is not a JSON array: %s", literal)
	}

	trimmed = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	if trimmed == "" {
		return nil, nil
	}

	var out []string

	for part := range strings.SplitSeq(trimmed, ",") {
		value, err := strconv.Unquote(strings.TrimSpace(part))
		if err != nil {
			return nil, fmt.Errorf("STRICT_SOMETIMES entry %s: %w", part, err)
		}

		out = append(out, value)
	}

	sort.Strings(out)

	return out, nil
}

// toleratedSometimesMessages collects the literal message of every
// assert.Sometimes whose condition is an IsTolerated(...) call.
func toleratedSometimesMessages(files []string) ([]string, error) {
	var out []string

	for _, path := range files {
		if !strings.HasPrefix(path, strictSometimesDir+"/") ||
			!strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			continue
		}

		source, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", path, err)
		}

		messages, err := toleratedSometimesInSource(path, source)
		if err != nil {
			return nil, err
		}

		out = append(out, messages...)
	}

	sort.Strings(out)

	return out, nil
}

func toleratedSometimesInSource(path string, source []byte) ([]string, error) {
	parsed, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}

	var out []string

	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || !isSelector(call.Fun, "assert", "Sometimes") || len(call.Args) < 2 {
			return true
		}

		cond, ok := call.Args[0].(*ast.CallExpr)
		if !ok || !isIdent(cond.Fun, "IsTolerated") {
			return true
		}

		if msg, ok := call.Args[1].(*ast.BasicLit); ok && msg.Kind == token.STRING {
			if value, err := strconv.Unquote(msg.Value); err == nil {
				out = append(out, value)
			}
		}

		return true
	})

	return out, nil
}

func isSelector(expr ast.Expr, pkg, name string) bool {
	sel, ok := expr.(*ast.SelectorExpr)

	return ok && sel.Sel.Name == name && isIdent(sel.X, pkg)
}

func isIdent(expr ast.Expr, name string) bool {
	ident, ok := expr.(*ast.Ident)

	return ok && ident.Name == name
}
