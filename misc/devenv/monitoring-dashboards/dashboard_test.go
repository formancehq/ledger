package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var nativeClassicHistogramSuffix = regexp.MustCompile(`(?:raft|admission|wal|pebble|http)[A-Za-z0-9_]*(?:_sum|_count)(?:\{|\[)`)

func TestGeneratedDashboards(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("config/dashboards/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 7 {
		t.Fatalf("expected 7 generated dashboard variants, got %d", len(files))
	}
	sort.Strings(files)

	for _, file := range files {
		file := file
		t.Run(filepath.Base(file), func(t *testing.T) {
			t.Parallel()

			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}

			var dashboard map[string]any
			if err := json.Unmarshal(raw, &dashboard); err != nil {
				t.Fatalf("invalid dashboard JSON: %v", err)
			}

			assertDatasourceDefault(t, dashboard, strings.Contains(file, "-native"))
			assertDashboardTree(t, dashboard, strings.Contains(file, "-native"))
		})
	}
}

func assertDatasourceDefault(t *testing.T, dashboard map[string]any, native bool) {
	t.Helper()

	templating := objectField(t, dashboard, "templating")
	variables := arrayField(t, templating, "list")
	if len(variables) != 4 {
		t.Fatalf("expected datasource, Pyroscope, cluster and node variables; got %d", len(variables))
	}

	datasource := variables[0].(map[string]any)
	current := objectField(t, datasource, "current")
	want := "Prometheus"
	if native {
		want = "Prometheus Native"
	}
	if got := current["value"]; got != want {
		t.Errorf("datasource default = %v, want %q", got, want)
	}

	for _, raw := range variables {
		variable := raw.(map[string]any)
		if variable["name"] == "version" {
			t.Error("version variable must not be generated: live Ledger profiles do not expose that label")
		}
	}
}

func assertDashboardTree(t *testing.T, value any, native bool) {
	t.Helper()

	panelTitles := map[string]struct{}{}
	var walk func(any, string)
	walk = func(value any, path string) {
		switch value := value.(type) {
		case map[string]any:
			if _, hasTargets := value["targets"]; hasTargets {
				if title, ok := value["title"].(string); ok {
					if _, duplicate := panelTitles[title]; duplicate {
						t.Errorf("duplicate panel title %q", title)
					}
					panelTitles[title] = struct{}{}
				}
			}

			if expr, ok := value["expr"].(string); ok {
				if strings.TrimSpace(expr) == "" {
					t.Errorf("empty Prometheus expression at %s", path)
				}
				if !strings.Contains(expr, "$cluster") {
					t.Errorf("unscoped Prometheus expression at %s: %s", path, expr)
				}
				if strings.Contains(expr, "scope.name") || strings.Contains(expr, "scope_name") ||
					strings.Contains(expr, "scope.attributes") || strings.Contains(expr, "scope_attributes") {
					t.Errorf("expression relies on dropped instrumentation-scope labels at %s: %s", path, expr)
				}
				if strings.Contains(expr, "wal.append.cache") || strings.Contains(expr, "wal_append_cache") {
					t.Errorf("expression references removed WAL cache metric at %s: %s", path, expr)
				}
				if native && nativeClassicHistogramSuffix.MatchString(expr) {
					t.Errorf("native dashboard references a classic histogram suffix at %s: %s", path, expr)
				}
			}

			if profileType, ok := value["profileTypeId"].(string); ok && strings.HasPrefix(profileType, "goroutine:") {
				t.Errorf("obsolete singular goroutine profile type at %s: %s", path, profileType)
			}
			if selector, ok := value["labelSelector"].(string); ok && strings.Contains(selector, "version=") {
				t.Errorf("Pyroscope selector relies on absent version label at %s: %s", path, selector)
			}

			for key, child := range value {
				walk(child, fmt.Sprintf("%s.%s", path, key))
			}
		case []any:
			for index, child := range value {
				walk(child, fmt.Sprintf("%s[%d]", path, index))
			}
		}
	}

	walk(value, "dashboard")
}

func objectField(t *testing.T, object map[string]any, field string) map[string]any {
	t.Helper()
	value, ok := object[field].(map[string]any)
	if !ok {
		t.Fatalf("field %q is not an object", field)
	}
	return value
}

func arrayField(t *testing.T, object map[string]any, field string) []any {
	t.Helper()
	value, ok := object[field].([]any)
	if !ok {
		t.Fatalf("field %q is not an array", field)
	}
	return value
}
