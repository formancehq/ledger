package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type finding struct {
	path    string
	line    int
	column  int
	message string
}

func main() {
	fuzzInventoryOnly := len(os.Args) == 2 && os.Args[1] == "--fuzz-inventory"
	if len(os.Args) > 1 && !fuzzInventoryOnly {
		fmt.Fprintln(os.Stderr, "usage: check-repo-invariants [--fuzz-inventory]")
		os.Exit(2)
	}

	files, err := repositoryFiles()
	if err != nil {
		fmt.Fprintf(os.Stderr, "check-repo-invariants: listing repository files: %v\n", err)
		os.Exit(1)
	}

	failed := false

	if !fuzzInventoryOnly {
		for _, path := range files {
			findings, err := checkFile(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "check-repo-invariants: checking %s: %v\n", path, err)
				failed = true

				continue
			}

			for _, item := range findings {
				printFinding(item)
				failed = true
			}
		}
	}

	fuzzFindings, err := checkFuzzInventory(files)
	if err != nil {
		fmt.Fprintf(os.Stderr, "check-repo-invariants: checking fuzz inventory: %v\n", err)
		failed = true
	} else {
		for _, item := range fuzzFindings {
			printFinding(item)
			failed = true
		}
	}

	if failed {
		fmt.Fprintln(os.Stderr, "check-repo-invariants: FAIL")
		os.Exit(1)
	}

	fmt.Println("check-repo-invariants: PASS")
}

func printFinding(item finding) {
	fmt.Printf("%s:%d:%d: ERROR: %s\n", item.path, item.line, item.column, item.message)
}

func repositoryFiles() ([]string, error) {
	cmd := exec.Command("git", "ls-files", "--cached", "--others", "--exclude-standard", "-z")

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("running git ls-files: %w", err)
	}

	parts := bytes.Split(output, []byte{0})
	files := make([]string, 0, len(parts))

	for _, part := range parts {
		if len(part) == 0 {
			continue
		}

		files = append(files, string(part))
	}

	return files, nil
}

func checkFile(path string) ([]finding, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("stating file: %w", err)
	}

	if !info.Mode().IsRegular() {
		return nil, nil
	}

	source, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	switch {
	case path == defaultCIWorkflowPath:
		return checkModelWorkloadTestReachability(path, source)
	case strings.HasSuffix(path, ".go"):
		return checkGoSource(path, source)
	case isProtoPath(path):
		return checkProtoSource(path, source), nil
	default:
		return nil, nil
	}
}

func checkGoSource(path string, source []byte) ([]finding, error) {
	checkSleep := strings.HasSuffix(path, "_test.go")
	checkEnvironment := isDeterministicFSMPath(path) && !checkSleep
	checkBoundaryImport := isBusinessCorePath(path)
	if !checkSleep && !checkEnvironment && !checkBoundaryImport {
		return nil, nil
	}

	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, source, 0)
	if err != nil {
		return nil, fmt.Errorf("parsing Go source: %w", err)
	}

	var (
		timeNames = map[string]struct{}{}
		osNames   = map[string]struct{}{}
		timeDot   bool
		osDot     bool
	)

	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return nil, fmt.Errorf("decoding import %q: %w", spec.Path.Value, err)
		}

		name := filepath.Base(importPath)
		if spec.Name != nil {
			name = spec.Name.Name
		}

		switch importPath {
		case "time":
			timeDot = name == "."
			if name != "." && name != "_" {
				timeNames[name] = struct{}{}
			}
		case "os":
			osDot = name == "."
			if name != "." && name != "_" {
				osNames[name] = struct{}{}
			}
		}
	}

	var findings []finding

	if checkBoundaryImport {
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return nil, fmt.Errorf("decoding import %q: %w", spec.Path.Value, err)
			}

			if importPath != apierrPackage {
				continue
			}

			findings = append(findings, goFinding(
				fileSet,
				path,
				spec.Path.Pos(),
				"the business core must not import "+apierrPackage+
					"; a failure decoded from a peer is an adapter-boundary representation and"+
					" must never be raised by admission, the FSM or order processing, nor persisted,"+
					" frozen, audited or hashed",
			))
		}
	}

	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}

		if checkSleep && callsImportedFunction(call.Fun, timeNames, timeDot, "Sleep") {
			findings = append(findings, goFinding(
				fileSet,
				path,
				call.Fun.Pos(),
				"tests must not use time.Sleep; use deterministic synchronization or require.Eventually",
			))
		}

		if checkEnvironment && callsAnyImportedFunction(
			call.Fun,
			osNames,
			osDot,
			"Getenv",
			"LookupEnv",
			"Environ",
		) {
			findings = append(findings, goFinding(
				fileSet,
				path,
				call.Fun.Pos(),
				"deterministic FSM paths must not read node-local environment state directly; gate local policy in admission instead",
			))
		}

		return true
	})

	return findings, nil
}

func callsAnyImportedFunction(
	expression ast.Expr,
	packageNames map[string]struct{},
	dotImport bool,
	functionNames ...string,
) bool {
	for _, name := range functionNames {
		if callsImportedFunction(expression, packageNames, dotImport, name) {
			return true
		}
	}

	return false
}

func callsImportedFunction(
	expression ast.Expr,
	packageNames map[string]struct{},
	dotImport bool,
	functionName string,
) bool {
	switch expression := expression.(type) {
	case *ast.SelectorExpr:
		identifier, ok := expression.X.(*ast.Ident)
		if !ok || identifier.Obj != nil || expression.Sel.Name != functionName {
			return false
		}

		_, ok = packageNames[identifier.Name]

		return ok
	case *ast.Ident:
		return dotImport && expression.Obj == nil && expression.Name == functionName
	default:
		return false
	}
}

func goFinding(fileSet *token.FileSet, path string, position token.Pos, message string) finding {
	location := fileSet.Position(position)

	return finding{
		path:    path,
		line:    location.Line,
		column:  location.Column,
		message: message,
	}
}

func checkProtoSource(path string, source []byte) []finding {
	masked := maskProtoCommentsAndStrings(source)
	declaration := regexp.MustCompile(`(?m)(?:^|[;{}])[ \t\r\n]*reserved(?:[ \t\r\n]|$)`)

	var findings []finding

	for _, indexes := range declaration.FindAllIndex(masked, -1) {
		segment := masked[indexes[0]:indexes[1]]
		relative := bytes.Index(segment, []byte("reserved"))
		if relative < 0 {
			continue
		}

		offset := indexes[0] + relative
		line, column := lineAndColumn(source, offset)
		findings = append(findings, finding{
			path:    path,
			line:    line,
			column:  column,
			message: "Ledger v3 is unreleased; protobuf reserved declarations are intentionally forbidden by AGENTS.md",
		})
	}

	return findings
}

func maskProtoCommentsAndStrings(source []byte) []byte {
	masked := append([]byte(nil), source...)

	const (
		stateCode = iota
		stateLineComment
		stateBlockComment
		stateString
	)

	state := stateCode
	var quote byte

	for index := 0; index < len(masked); index++ {
		current := masked[index]

		switch state {
		case stateCode:
			switch {
			case current == '/' && index+1 < len(masked) && masked[index+1] == '/':
				masked[index] = ' '
				masked[index+1] = ' '
				index++
				state = stateLineComment
			case current == '/' && index+1 < len(masked) && masked[index+1] == '*':
				masked[index] = ' '
				masked[index+1] = ' '
				index++
				state = stateBlockComment
			case current == '\'' || current == '"':
				quote = current
				masked[index] = ' '
				state = stateString
			}
		case stateLineComment:
			if current == '\n' {
				state = stateCode
			} else {
				masked[index] = ' '
			}
		case stateBlockComment:
			if current == '*' && index+1 < len(masked) && masked[index+1] == '/' {
				masked[index] = ' '
				masked[index+1] = ' '
				index++
				state = stateCode
			} else if current != '\n' {
				masked[index] = ' '
			}
		case stateString:
			switch {
			case current == '\\' && index+1 < len(masked):
				masked[index] = ' '
				index++
				if masked[index] != '\n' {
					masked[index] = ' '
				}
			case current == quote:
				masked[index] = ' '
				state = stateCode
			case current != '\n':
				masked[index] = ' '
			}
		}
	}

	return masked
}

func lineAndColumn(source []byte, offset int) (int, int) {
	line := bytes.Count(source[:offset], []byte{'\n'}) + 1
	lastNewline := bytes.LastIndex(source[:offset], []byte{'\n'})

	return line, offset - lastNewline
}

// apierrPackage is the API boundary representation of a failure decoded from a
// peer. It is legitimate at an adapter boundary and nowhere else.
const apierrPackage = "github.com/formancehq/ledger/v3/internal/adapter/apierr"

// isBusinessCorePath reports whether path belongs to the business core: the
// packages that raise, freeze, audit and hash business failures.
//
// EN-1980: a decoded remote failure carries a reason and message belonging to
// the *sending* build. The audit chain hashes an error's Error() string, so a
// message that varies with a peer's version would break the chain; a frozen
// idempotency outcome replays a reason that must stay enum-bound. Keeping the
// boundary type out of these trees is a dependency rule, not a type-system
// guarantee — this check is what enforces it. internal/domain is listed for
// completeness: apierr imports it, so that direction is already a compile
// error, and the rule states the intent rather than relying on the cycle.
func isBusinessCorePath(path string) bool {
	for _, prefix := range []string{
		"internal/domain/",
		"internal/application/admission/",
		"internal/infra/state/",
		"internal/infra/plan/",
		"internal/infra/preload/",
	} {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}

	return false
}

func isDeterministicFSMPath(path string) bool {
	for _, prefix := range []string{
		"internal/infra/state/",
		"internal/infra/plan/",
		"internal/infra/preload/",
		"internal/domain/processing/",
	} {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}

	return false
}

func isProtoPath(path string) bool {
	return strings.HasPrefix(path, "misc/proto/") && strings.HasSuffix(path, ".proto")
}
