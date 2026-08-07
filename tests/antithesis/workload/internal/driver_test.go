package internal

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOperationalSingletonEntrypointsAreFinite(t *testing.T) {
	t.Parallel()

	files := []string{
		"singleton_driver_chapter_close/main.go",
		"singleton_driver_config_change/main.go",
		"singleton_driver_quorum_recovery/main.go",
		"singleton_driver_rolling_restart/main.go",
		"singleton_driver_scenario_blocks/main.go",
		"singleton_driver_scaling/main.go",
		"singleton_driver_scaling_chaos/main.go",
		"singleton_driver_scaling_structured/main.go",
	}

	for _, name := range files {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join("../bin/cmds/main", name)
			source, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}

			mainFn := findFunction(file, "main")
			if mainFn == nil {
				t.Fatalf("%s has no main function", path)
			}

			ast.Inspect(mainFn.Body, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.ForStmt:
					if node.Cond == nil {
						t.Errorf("%s main contains an unbounded for loop; singleton commands must finish", path)
					}
				case *ast.CallExpr:
					selector, ok := node.Fun.(*ast.SelectorExpr)
					if ok && selector.Sel.Name == "RunImmediatelyThenEvery" {
						t.Errorf("%s main calls RunImmediatelyThenEvery; singleton commands must run once and finish", path)
					}
				}

				return true
			})
		})
	}
}

func TestScenarioBlockRunnerIsFinite(t *testing.T) {
	t.Parallel()

	path := "block/block.go"
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	runLoop := findFunction(file, "RunLoop")
	if runLoop == nil {
		t.Fatalf("%s has no RunLoop function", path)
	}
	ast.Inspect(runLoop.Body, func(node ast.Node) bool {
		if loop, ok := node.(*ast.ForStmt); ok && loop.Cond == nil {
			t.Errorf("%s RunLoop contains an unbounded loop; the singleton command must finish", path)
		}

		return true
	})
}

func TestLongOperationalSingletonsUsePlatformBound(t *testing.T) {
	t.Parallel()

	files := []string{
		"singleton_driver_quorum_recovery/main.go",
		"singleton_driver_scaling_structured/main.go",
		"singleton_driver_scenario_blocks/main.go",
	}
	for _, name := range files {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join("../bin/cmds/main", name)
			source, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			if !strings.Contains(string(source), "internal.PlatformSingletonContext()") {
				t.Errorf("%s must use PlatformSingletonContext so a 20-minute run can observe completion", path)
			}
		})
	}
}

func TestK8sModelDriverHasAPlatformRuntimeBound(t *testing.T) {
	t.Parallel()

	manifest, err := os.ReadFile("../../k8s/workload.yaml")
	if err != nil {
		t.Fatalf("read workload manifest: %v", err)
	}
	if !strings.Contains(string(manifest), "- name: MODEL_MAX_SECONDS\n          value: \"600\"") {
		t.Error("k8s model driver must stop within 10 minutes so a 20-minute Antithesis run can observe command completion")
	}
}

func findFunction(file *ast.File, name string) *ast.FuncDecl {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == name {
			return function
		}
	}

	return nil
}
