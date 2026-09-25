package main

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

// A body behind `if assert.Enabled` is compiled out of every default build, so
// no ordinary test can execute it and no ordinary run can count it. Only
// `just test-antithesis-assertions` builds it, and that target names the
// package trees it runs rather than the whole module — deliberately, since an
// armed run of everything costs far more than the guarded code is worth.
//
// The consequence is that a guard added outside those trees is invisible
// twice: never executed armed, and missing from every uploaded coverage
// profile while each gate stays green. That is the failure this whole change
// is about, so the target's package list and the guards it must reach are
// pinned to each other here.
const (
	justfilePath        = "justfile"
	armedCoverageRecipe = "test-antithesis-assertions"
)

// The workload is a separate module with its own armed build: the
// Tests-Antithesis-Workload job runs it with the tag — which
// check_model_workload_reachability pins — and its Dockerfile builds the
// campaign binaries the same way. A guard there does execute armed, so the
// justfile recipe is not the authority over it.
var armedCoverageExternalTrees = []string{"tests/antithesis/workload"}

var (
	armedCoverageGuard  = regexp.MustCompile(`(?m)^\s*if\s+assert\.Enabled\s*{`)
	armedCoveragePkgArg = regexp.MustCompile(`\./([A-Za-z0-9_./-]+?)/\.\.\.`)
)

// checkArmedCoverage reports guards that the armed test target cannot reach.
func checkArmedCoverage(files []string) ([]finding, error) {
	recipeTrees, err := armedCoverageTrees()
	if err != nil {
		return nil, err
	}

	if len(recipeTrees) == 0 {
		return []finding{{
			path: justfilePath, line: 1, column: 1,
			message: fmt.Sprintf(
				"ARMED_COVERAGE_TARGET_MISSING: no package trees found in the %s recipe",
				armedCoverageRecipe,
			),
		}}, nil
	}

	trees := append(append([]string{}, recipeTrees...), armedCoverageExternalTrees...)

	var findings []finding

	for _, path := range files {
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			continue
		}

		source, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", path, err)
		}

		if item, found := armedCoverageInSource(path, source, trees); found {
			findings = append(findings, item)
		}
	}

	return findings, nil
}

// armedCoverageInSource reports one file that guards code the armed target
// cannot reach. A _test.go file is compiled by that target's own test build,
// so only ordinary sources are subject to this.
func armedCoverageInSource(path string, source []byte, trees []string) (finding, bool) {
	if strings.HasSuffix(path, "_test.go") || !armedCoverageGuard.Match(source) {
		return finding{}, false
	}

	for _, tree := range trees {
		if strings.HasPrefix(path, tree+"/") {
			return finding{}, false
		}
	}

	return finding{
		path: path, line: 1, column: 1,
		message: fmt.Sprintf(
			"ARMED_COVERAGE_UNREACHABLE: %s holds an `if assert.Enabled` guard outside every armed build (%s); "+
				"the guarded lines would never execute armed and never appear in a coverage profile",
			path, strings.Join(trees, " "),
		),
	}, true
}

// armedCoverageTrees returns the package trees the armed target runs, as
// repository-relative directories.
func armedCoverageTrees() ([]string, error) {
	source, err := os.ReadFile(justfilePath)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", justfilePath, err)
	}

	return armedCoverageTreesFromJustfile(string(source))
}

// armedCoverageTreesFromJustfile extracts the package trees the armed recipe
// runs, so widening the recipe automatically widens what this check accepts.
func armedCoverageTreesFromJustfile(source string) ([]string, error) {
	body, found := justRecipeBody(source, armedCoverageRecipe)
	if !found {
		return nil, fmt.Errorf("recipe %q not found in %s", armedCoverageRecipe, justfilePath)
	}

	seen := map[string]struct{}{}

	for _, match := range armedCoveragePkgArg.FindAllStringSubmatch(body, -1) {
		seen[match[1]] = struct{}{}
	}

	trees := make([]string, 0, len(seen))
	for tree := range seen {
		trees = append(trees, tree)
	}

	sort.Strings(trees)

	return trees, nil
}

// justRecipeBody returns the indented body of one just recipe.
func justRecipeBody(source, recipe string) (string, bool) {
	lines := strings.Split(source, "\n")

	start := -1

	for i, line := range lines {
		if strings.HasPrefix(line, recipe+":") {
			start = i + 1

			break
		}
	}

	if start < 0 {
		return "", false
	}

	var body []string

	for _, line := range lines[start:] {
		if strings.TrimSpace(line) != "" && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") {
			break
		}

		body = append(body, line)
	}

	return strings.Join(body, "\n"), true
}
