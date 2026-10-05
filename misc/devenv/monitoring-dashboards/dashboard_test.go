package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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
	if len(files) != 8 {
		t.Fatalf("expected 8 generated dashboard variants, got %d", len(files))
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
			assertMetricPrefix(t, string(raw), filepath.Base(file))
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

// assertMetricPrefix checks that the server-side metrics prefix is
// applied to the ledger's own metrics in prefixed variants, absent from
// -noprefix variants, and never applied to OpenTelemetry
// semantic-convention metrics (go.*, process.*, system.*, http.*, …),
// which bypass the go-libs prefixed provider.
func assertMetricPrefix(t *testing.T, raw, file string) {
	t.Helper()

	prefix := "formance_ledger_"
	if strings.HasPrefix(file, "ledger-metrics-otel") {
		prefix = "formance.ledger."
	}
	if strings.Contains(file, "-noprefix") {
		if strings.Contains(raw, "formance_ledger") || strings.Contains(raw, "formance.ledger") {
			t.Errorf("-noprefix variant references the metrics prefix")
		}

		return
	}
	if !strings.Contains(raw, prefix+"raft") {
		t.Errorf("prefixed variant does not reference %sraft* metrics", prefix)
	}
	for _, semconv := range []string{"go", "process", "system", "http", "rpc"} {
		if strings.Contains(raw, prefix+semconv+string(prefix[len(prefix)-1])) {
			t.Errorf("semantic-convention metric %s* must not be prefixed", semconv)
		}
	}
}

// TestPrefixedVariantsMatchNoPrefixCounterparts checks that every
// prefixed variant equals its -noprefix counterpart once the prefix is
// removed, so the prefix is applied to every ledger metric and to
// nothing else (labels, functions, semantic-convention metrics).
func TestPrefixedVariantsMatchNoPrefixCounterparts(t *testing.T) {
	t.Parallel()

	pairs := map[string]string{
		"ledger-metrics-otel.json":                   "ledger-metrics-otel-noprefix.json",
		"ledger-metrics-prom.json":                   "ledger-metrics-prom-noprefix.json",
		"ledger-metrics-prom-normalized.json":        "ledger-metrics-prom-noprefix-normalized.json",
		"ledger-metrics-prom-normalized-native.json": "ledger-metrics-prom-noprefix-normalized-native.json",
	}
	load := func(t *testing.T, file string, stripPrefix bool) map[string]any {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join("config", "dashboards", file))
		if err != nil {
			t.Fatal(err)
		}
		content := string(raw)
		if stripPrefix {
			content = strings.ReplaceAll(content, "formance.ledger.", "")
			content = strings.ReplaceAll(content, "formance_ledger_", "")
		}
		var dashboard map[string]any
		if err := json.Unmarshal([]byte(content), &dashboard); err != nil {
			t.Fatalf("invalid dashboard JSON in %s: %v", file, err)
		}
		delete(dashboard, "title")
		delete(dashboard, "uid")

		return dashboard
	}

	namespaces := ledgerMetricNamespaces(t)
	for prefixed, unprefixed := range pairs {
		t.Run(prefixed, func(t *testing.T) {
			t.Parallel()

			if !reflect.DeepEqual(load(t, prefixed, true), load(t, unprefixed, false)) {
				t.Errorf("%s with the prefix removed differs from %s", prefixed, unprefixed)
			}

			// Stripping the prefix everywhere hides where it was applied,
			// so also compare the query tokens pairwise: a ledger metric
			// must gain exactly the prefix, anything else (labels,
			// functions, semantic-convention metrics) must not.
			prefix := "formance_ledger_"
			separator := "_"
			if strings.HasPrefix(prefixed, "ledger-metrics-otel") {
				prefix, separator = "formance.ledger.", "."
			}
			assertQueryTokens(t, load(t, prefixed, false), load(t, unprefixed, false), prefix, separator, namespaces, "dashboard")
		})
	}
}

// queryToken matches a PromQL identifier or a quoted metric/label name.
var queryToken = regexp.MustCompile(`[A-Za-z_:][A-Za-z0-9_:.]*`)

// labelValue matches the quoted right-hand side of a label matcher.
var labelValue = regexp.MustCompile(`(=~|!~|!=|=)\s*"[^"]*"`)

// ledgerMetricNamespaces returns the first segment of every metric name
// in the dashboard registry (admission, raft, wal, …): the namespaces of
// the metrics the ledger itself emits and therefore prefixes.
func ledgerMetricNamespaces(t *testing.T) map[string]struct{} {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("jsonnet", "lib", "metrics.libsonnet"))
	if err != nil {
		t.Fatal(err)
	}
	namespaces := map[string]struct{}{}
	for _, m := range regexp.MustCompile(`'([a-z][a-z0-9_]*)(?:\.[a-z][a-z0-9_]*)+'`).FindAllStringSubmatch(string(raw), -1) {
		namespaces[m[1]] = struct{}{}
	}
	if len(namespaces) == 0 {
		t.Fatal("no metric namespaces found in metrics.libsonnet")
	}

	return namespaces
}

// assertQueryTokens walks two dashboards with the same structure and
// compares the PromQL-bearing fields token by token.
func assertQueryTokens(t *testing.T, prefixed, unprefixed any, prefix, separator string, namespaces map[string]struct{}, path string) {
	t.Helper()

	switch p := prefixed.(type) {
	case map[string]any:
		u, ok := unprefixed.(map[string]any)
		if !ok {
			t.Fatalf("structure mismatch at %s", path)
		}
		for key, pv := range p {
			ps, isString := pv.(string)
			if isString && (key == "expr" || key == "query" || key == "definition") {
				us, _ := u[key].(string)
				assertExprTokens(t, ps, us, prefix, separator, namespaces, path+"."+key)

				continue
			}
			assertQueryTokens(t, pv, u[key], prefix, separator, namespaces, path+"."+key)
		}
	case []any:
		u, ok := unprefixed.([]any)
		if !ok || len(u) != len(p) {
			t.Fatalf("structure mismatch at %s", path)
		}
		for i := range p {
			assertQueryTokens(t, p[i], u[i], prefix, separator, namespaces, fmt.Sprintf("%s[%d]", path, i))
		}
	}
}

func assertExprTokens(t *testing.T, prefixed, unprefixed, prefix, separator string, namespaces map[string]struct{}, path string) {
	t.Helper()

	// Label values (`volume="wal"`) are user data, never names.
	pTokens := queryToken.FindAllString(labelValue.ReplaceAllString(prefixed, `$1""`), -1)
	uTokens := queryToken.FindAllString(labelValue.ReplaceAllString(unprefixed, `$1""`), -1)
	if len(pTokens) != len(uTokens) {
		t.Errorf("token count mismatch at %s:\n  %s\n  %s", path, prefixed, unprefixed)

		return
	}
	for i, u := range uTokens {
		_, isLedgerMetric := namespaces[strings.SplitN(u, separator, 2)[0]]
		switch {
		case pTokens[i] == prefix+u && !isLedgerMetric:
			t.Errorf("non-ledger token %q is prefixed at %s: %s", u, path, prefixed)
		case pTokens[i] == u && isLedgerMetric:
			t.Errorf("ledger metric %q is missing the prefix at %s: %s", u, path, prefixed)
		case pTokens[i] != u && pTokens[i] != prefix+u:
			t.Errorf("token %q differs unexpectedly from %q at %s", pTokens[i], u, path)
		}
	}
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
