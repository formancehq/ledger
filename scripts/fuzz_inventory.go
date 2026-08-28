package main

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const fuzzRunnerInventoryPath = "scripts/fuzz-targets.txt"

type fuzzTarget struct {
	packagePath string
	name        string
}

type locatedFuzzTarget struct {
	target   fuzzTarget
	location finding
}

func checkFuzzInventory(files []string) ([]finding, error) {
	actual, err := discoverRepositoryFuzzTargets(files)
	if err != nil {
		return nil, err
	}

	runner, findings, err := readFuzzRunnerInventory(fuzzRunnerInventoryPath)
	if err != nil {
		return nil, err
	}

	return append(findings, compareFuzzTargets(actual, runner)...), nil
}

func discoverRepositoryFuzzTargets(files []string) ([]locatedFuzzTarget, error) {
	var targets []locatedFuzzTarget

	for _, path := range files {
		if !strings.HasSuffix(path, "_test.go") {
			continue
		}

		source, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", path, err)
		}

		declared, err := discoverFuzzTargets(path, source)
		if err != nil {
			return nil, err
		}

		targets = append(targets, declared...)
	}

	sort.Slice(targets, func(left, right int) bool {
		return fuzzTargetKey(targets[left].target) < fuzzTargetKey(targets[right].target)
	})

	return targets, nil
}

func discoverFuzzTargets(path string, source []byte) ([]locatedFuzzTarget, error) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, source, 0)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}

	testingNames := map[string]struct{}{}
	dotImport := false

	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return nil, fmt.Errorf("decoding import %q in %s: %w", spec.Path.Value, path, err)
		}
		if importPath != "testing" {
			continue
		}

		name := "testing"
		if spec.Name != nil {
			name = spec.Name.Name
		}
		if name == "." {
			dotImport = true
		} else if name != "_" {
			testingNames[name] = struct{}{}
		}
	}

	packagePath := "./"
	if directory := filepath.ToSlash(filepath.Dir(path)); directory != "." {
		packagePath += strings.TrimSuffix(directory, "/") + "/"
	}

	var targets []locatedFuzzTarget

	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || !isFuzzFunction(function, testingNames, dotImport) {
			continue
		}

		location := fileSet.Position(function.Name.Pos())
		targets = append(targets, locatedFuzzTarget{
			target: fuzzTarget{
				packagePath: packagePath,
				name:        function.Name.Name,
			},
			location: finding{
				path:   path,
				line:   location.Line,
				column: location.Column,
			},
		})
	}

	return targets, nil
}

func isFuzzFunction(function *ast.FuncDecl, testingNames map[string]struct{}, dotImport bool) bool {
	if function.Recv != nil || function.Type.TypeParams != nil || function.Type.Results != nil ||
		!isGoTestName(function.Name.Name, "Fuzz") || function.Type.Params.NumFields() != 1 {
		return false
	}

	field := function.Type.Params.List[0]
	if len(field.Names) > 1 {
		return false
	}

	pointer, ok := field.Type.(*ast.StarExpr)
	if !ok {
		return false
	}

	switch expression := pointer.X.(type) {
	case *ast.SelectorExpr:
		identifier, ok := expression.X.(*ast.Ident)
		if !ok || identifier.Obj != nil || expression.Sel.Name != "F" {
			return false
		}
		_, ok = testingNames[identifier.Name]

		return ok
	case *ast.Ident:
		return dotImport && expression.Name == "F"
	default:
		return false
	}
}

func isGoTestName(name, prefix string) bool {
	if !strings.HasPrefix(name, prefix) || len(name) == len(prefix) {
		return false
	}

	next, _ := utf8.DecodeRuneInString(name[len(prefix):])

	return !unicode.IsLower(next)
}

func readFuzzRunnerInventory(path string) ([]locatedFuzzTarget, []finding, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("opening runner inventory %s: %w", path, err)
	}
	defer func() {
		_ = file.Close() // Best effort after the scanner has consumed the file.
	}()

	var (
		targets  []locatedFuzzTarget
		findings []finding
		previous string
	)

	scanner := bufio.NewScanner(file)
	for line := 1; scanner.Scan(); line++ {
		text := scanner.Text()
		fields := strings.Fields(text)
		location := finding{path: path, line: line, column: 1}

		if len(fields) != 2 {
			location.message = "active fuzz runner entries must contain exactly: <package> <FuzzTarget>"
			findings = append(findings, location)

			continue
		}

		packagePath, name := fields[0], fields[1]
		cleanPackagePath := "./" + strings.TrimPrefix(filepath.ToSlash(filepath.Clean(packagePath)), "./") + "/"
		if !strings.HasPrefix(packagePath, "./") || !strings.HasSuffix(packagePath, "/") ||
			cleanPackagePath != packagePath {
			location.message = fmt.Sprintf("active fuzz runner package %q must be a clean ./-relative path ending in /", packagePath)
			findings = append(findings, location)

			continue
		}
		if !isGoTestName(name, "Fuzz") {
			location.message = fmt.Sprintf("active fuzz runner target %q is not a valid Go fuzz target name", name)
			findings = append(findings, location)

			continue
		}

		entry := packagePath + " " + name
		if previous != "" && entry <= previous {
			location.message = "active fuzz runner inventory must be unique and sorted by package and target name"
			findings = append(findings, location)
		}
		previous = entry

		targets = append(targets, locatedFuzzTarget{
			target:   fuzzTarget{packagePath: packagePath, name: name},
			location: location,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, fmt.Errorf("reading runner inventory %s: %w", path, err)
	}

	if len(targets) == 0 {
		findings = append(findings, finding{
			path:    path,
			line:    1,
			column:  1,
			message: "active fuzz runner inventory must not be empty",
		})
	}

	return targets, findings, nil
}

func compareFuzzTargets(actual, runner []locatedFuzzTarget) []finding {
	actualByKey := make(map[string]locatedFuzzTarget, len(actual))
	for _, target := range actual {
		actualByKey[fuzzTargetKey(target.target)] = target
	}

	runnerByKey := make(map[string]locatedFuzzTarget, len(runner))
	for _, target := range runner {
		runnerByKey[fuzzTargetKey(target.target)] = target
	}

	var findings []finding

	for key, target := range actualByKey {
		if _, ok := runnerByKey[key]; ok {
			continue
		}

		location := target.location
		location.message = fmt.Sprintf(
			"Go fuzz target %s in %s is missing from %s; add %q",
			target.target.name,
			target.target.packagePath,
			fuzzRunnerInventoryPath,
			target.target.packagePath+" "+target.target.name,
		)
		findings = append(findings, location)
	}

	for key, target := range runnerByKey {
		if _, ok := actualByKey[key]; ok {
			continue
		}

		location := target.location
		location.message = fmt.Sprintf(
			"active fuzz runner target %s in %s has no matching Go fuzz declaration; remove or correct the entry",
			target.target.name,
			target.target.packagePath,
		)
		findings = append(findings, location)
	}

	sort.Slice(findings, func(left, right int) bool {
		if findings[left].path != findings[right].path {
			return findings[left].path < findings[right].path
		}
		if findings[left].line != findings[right].line {
			return findings[left].line < findings[right].line
		}

		return findings[left].column < findings[right].column
	})

	return findings
}

func fuzzTargetKey(target fuzzTarget) string {
	return target.packagePath + "\x00" + target.name
}
