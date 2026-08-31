package main

import (
	"bufio"
	"bytes"
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
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
	target            fuzzTarget
	location          finding
	unreachableReason string
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

	findings = append(findings, unreachableFuzzTargetFindings(actual)...)
	findings = append(findings, compareFuzzTargets(actual, runner)...)
	sortFindings(findings)

	return findings, nil
}

func discoverRepositoryFuzzTargets(files []string) ([]locatedFuzzTarget, error) {
	var (
		targets          []locatedFuzzTarget
		platformSuffixes map[string]struct{}
	)
	nestedModules := nestedGoModuleDirectories(files)

	for _, path := range files {
		if !strings.HasSuffix(path, "_test.go") {
			continue
		}

		source, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}

			return nil, fmt.Errorf("reading %s: %w", path, err)
		}

		declared, err := discoverFuzzTargets(path, source)
		if err != nil {
			return nil, err
		}

		if len(declared) == 0 {
			continue
		}
		var reason string
		if module := nestedGoModuleForPath(path, nestedModules); module != "" {
			reason = "the declaration belongs to nested Go module " + module
		} else {
			if platformSuffixes == nil {
				platformSuffixes, err = goPlatformSuffixes()
				if err != nil {
					return nil, err
				}
			}

			reason, err = fuzzFileConstraintReason(path, source, platformSuffixes)
			if err != nil {
				return nil, err
			}
		}
		for index := range declared {
			declared[index].unreachableReason = reason
		}

		targets = append(targets, declared...)
	}

	sort.Slice(targets, func(left, right int) bool {
		return fuzzTargetKey(targets[left].target) < fuzzTargetKey(targets[right].target)
	})

	return targets, nil
}

func nestedGoModuleDirectories(files []string) []string {
	var modules []string

	for _, path := range files {
		cleaned := filepath.ToSlash(filepath.Clean(path))
		if cleaned == "go.mod" || !strings.HasSuffix(cleaned, "/go.mod") {
			continue
		}

		modules = append(modules, strings.TrimSuffix(cleaned, "/go.mod"))
	}

	sort.Slice(modules, func(left, right int) bool {
		return len(modules[left]) > len(modules[right])
	})

	return modules
}

func nestedGoModuleForPath(path string, modules []string) string {
	cleaned := filepath.ToSlash(filepath.Clean(path))
	for _, module := range modules {
		if strings.HasPrefix(cleaned, module+"/") {
			return module
		}
	}

	return ""
}

func goPlatformSuffixes() (map[string]struct{}, error) {
	output, err := exec.Command("go", "tool", "dist", "list").Output()
	if err != nil {
		return nil, fmt.Errorf("listing Go platforms: %w", err)
	}

	operatingSystems := map[string]struct{}{}
	architectures := map[string]struct{}{}
	platforms := make([][2]string, 0)

	for line := range strings.FieldsSeq(string(output)) {
		parts := strings.Split(line, "/")
		if len(parts) != 2 {
			return nil, fmt.Errorf("decoding Go platform %q", line)
		}

		operatingSystems[parts[0]] = struct{}{}
		architectures[parts[1]] = struct{}{}
		platforms = append(platforms, [2]string{parts[0], parts[1]})
	}

	suffixes := make(map[string]struct{}, len(operatingSystems)+len(architectures)+len(platforms))
	for operatingSystem := range operatingSystems {
		suffixes["_"+operatingSystem] = struct{}{}
	}
	for architecture := range architectures {
		suffixes["_"+architecture] = struct{}{}
	}
	for _, platform := range platforms {
		suffixes["_"+platform[0]+"_"+platform[1]] = struct{}{}
	}

	return suffixes, nil
}

func fuzzFileConstraintReason(path string, source []byte, platformSuffixes map[string]struct{}) (string, error) {
	scanner := bufio.NewScanner(bytes.NewReader(source))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "package ") {
			break
		}
		if constraint.IsGoBuild(line) || constraint.IsPlusBuild(line) {
			return "the declaration file has a Go build constraint", nil
		}
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scanning build constraints in %s: %w", path, err)
	}

	stem := strings.TrimSuffix(filepath.Base(path), "_test.go")
	matchedSuffix := ""
	for suffix := range platformSuffixes {
		if strings.HasSuffix(stem, suffix) && len(suffix) > len(matchedSuffix) {
			matchedSuffix = suffix
		}
	}
	if matchedSuffix != "" {
		return "the declaration file has the platform suffix " + matchedSuffix, nil
	}

	return "", nil
}

func unreachableFuzzTargetFindings(targets []locatedFuzzTarget) []finding {
	var findings []finding

	for _, target := range targets {
		if target.unreachableReason == "" {
			continue
		}

		location := target.location
		resolution := "move it to an unconstrained test file or extend the runner explicitly"
		if strings.HasPrefix(target.unreachableReason, "the declaration belongs to nested Go module ") {
			resolution = "extend the runner to invoke targets from that module before adding the target"
		}
		location.message = fmt.Sprintf(
			"Go fuzz target %s is not reachable by the untagged, root-module active runner because %s; %s",
			target.target.name,
			target.unreachableReason,
			resolution,
		)
		findings = append(findings, location)
	}

	return findings
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
		cleanedPackagePath := filepath.ToSlash(filepath.Clean(packagePath))
		cleanPackagePath := "./"
		if cleanedPackagePath != "." {
			cleanPackagePath += strings.TrimPrefix(cleanedPackagePath, "./") + "/"
		}
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
		if target.unreachableReason != "" {
			continue
		}
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

	sortFindings(findings)

	return findings
}

func sortFindings(findings []finding) {
	sort.Slice(findings, func(left, right int) bool {
		if findings[left].path != findings[right].path {
			return findings[left].path < findings[right].path
		}
		if findings[left].line != findings[right].line {
			return findings[left].line < findings[right].line
		}

		return findings[left].column < findings[right].column
	})
}

func fuzzTargetKey(target fuzzTarget) string {
	return target.packagePath + "\x00" + target.name
}
