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

var (
	groupingClause = regexp.MustCompile(`\b(by|on|ignoring)\s*\(([^()]*)\)`)
	nodeLabel      = regexp.MustCompile(`formance[._]ledger[._]node[._]id`)
	clusterLabel   = regexp.MustCompile(`formance[._]ledger[._]cluster[._]name`)
	namespaceLabel = regexp.MustCompile(`k8s[._]namespace[._]name`)
)

var wellFormedLegend = regexp.MustCompile(`^(?:[^{}]|\{\{[A-Za-z_][A-Za-z0-9_.]*\}\})*$`)

var nativeClassicHistogramSuffix = regexp.MustCompile(`(?:raft|admission|wal|pebble|http)[A-Za-z0-9_]*(?:_sum|_count)(?:\{|\[)`)

// resourceAttributeLabels masks the ledger's own resource-attribute labels.
// They share the formance.ledger namespace with the metrics prefix but are
// label names, which --otel-metrics-prefix never touches, so the textual
// prefix checks must not see them.
var resourceAttributeLabels = strings.NewReplacer(
	"formance.ledger.cluster.id", "resource.cluster.id",
	"formance.ledger.cluster.name", "resource.cluster.name",
	"formance.ledger.node.id", "resource.node.id",
	"formance_ledger_cluster_id", "resource_cluster_id",
	"formance_ledger_cluster_name", "resource_cluster_name",
	"formance_ledger_node_id", "resource_node_id",
)

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
			assertClusterVariable(t, dashboard, string(raw), strings.HasPrefix(filepath.Base(file), "ledger-metrics-otel"))
		})
	}
}

// walkQueries calls fn with every PromQL-bearing field (expr, query,
// definition) of a dashboard tree.
func walkQueries(value any, path string, fn func(path, query string)) {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if s, ok := child.(string); ok && (key == "expr" || key == "query" || key == "definition") {
				fn(path+"."+key, s)

				continue
			}
			walkQueries(child, path+"."+key, fn)
		}
	case []any:
		for i, child := range v {
			walkQueries(child, fmt.Sprintf("%s[%d]", path, i), fn)
		}
	}
}

// clusterIDFilter matches a selector filtering the declared cluster ID on the
// Cluster variable, in any naming variant and JSON escaping.
var clusterIDFilter = regexp.MustCompile(`formance[._]ledger[._]cluster[._]id[^,}]{0,6}=~[^,}]{0,4}\$cluster`)

// assertClusterVariable applies the Cluster variable regex the way Grafana
// does to query_result lines. Declared cluster IDs repeat across clusters
// (EN-2031), so the variable must key on the cluster name: two clusters that
// share the ID "default" must still yield two values.
func assertClusterVariable(t *testing.T, dashboard map[string]any, raw string, otel bool) {
	t.Helper()

	if m := clusterIDFilter.FindString(raw); m != "" {
		t.Errorf("a query filters the cluster ID on $cluster (%s); key on formance.ledger.cluster.name", m)
	}
	// A Cluster resource name is only unique within its namespace, so every
	// query scoped to $cluster must also be scoped to $namespace.
	walkQueries(dashboard, "dashboard", func(path, query string) {
		if strings.Contains(query, "$cluster") && !strings.Contains(query, "$namespace") {
			t.Errorf("%s filters on $cluster without $namespace: %s", path, query)
		}
	})

	cluster := arrayField(t, objectField(t, dashboard, "templating"), "list")[3].(map[string]any)
	expr, _ := cluster["regex"].(string)
	pattern, err := regexp.Compile(strings.TrimSuffix(strings.TrimPrefix(expr, "/"), "/"))
	if err != nil {
		t.Fatalf("cluster variable regex %q: %v", expr, err)
	}
	label := func(name string) string {
		if otel {
			return name
		}

		return strings.ReplaceAll(name, ".", "_")
	}
	line := func(name string) string {
		return fmt.Sprintf(`raft.node.lead{%s="default", %s=%q, %s="2"} 1 1700000000000`,
			label("formance.ledger.cluster.id"), label("formance.ledger.cluster.name"), name, label("formance.ledger.node.id"))
	}
	for _, name := range []string{"prod-eu", "prod-us"} {
		match := pattern.FindStringSubmatch(line(name))
		if len(match) < 2 {
			t.Fatalf("cluster variable regex %q does not capture the cluster name in %q", expr, line(name))
		}
		if match[1] != name {
			t.Errorf("cluster variable on %q: got %q, want the cluster name %q", line(name), match[1], name)
		}
	}
}

func assertDatasourceDefault(t *testing.T, dashboard map[string]any, native bool) {
	t.Helper()

	templating := objectField(t, dashboard, "templating")
	variables := arrayField(t, templating, "list")
	if len(variables) != 5 {
		t.Fatalf("expected datasource, Pyroscope, namespace, cluster and node variables; got %d", len(variables))
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
				// Node IDs repeat across clusters, and the Namespace and
				// Cluster variables allow All: a node is only identified
				// together with its namespace and cluster name.
				for _, clause := range groupingClause.FindAllStringSubmatch(expr, -1) {
					if nodeLabel.MatchString(clause[2]) && (!clusterLabel.MatchString(clause[2]) || !namespaceLabel.MatchString(clause[2])) {
						t.Errorf("%s clause groups by node without namespace and cluster name at %s: %s", clause[1], path, clause[0])
					}
				}
			}
			if legend, ok := value["legendFormat"].(string); ok && nodeLabel.MatchString(legend) && !clusterLabel.MatchString(legend) {
				t.Errorf("legend names a node without its cluster at %s: %q", path, legend)
			}

			// Grafana substitutes {{label}}; rewriteLegendFormat only
			// de-dots that exact form, so anything else leaves a raw
			// or empty placeholder in Prometheus variants.
			if legend, ok := value["legendFormat"].(string); ok && !wellFormedLegend.MatchString(legend) {
				t.Errorf("malformed legend placeholder at %s: %q", path, legend)
			}

			if profileType, ok := value["profileTypeId"].(string); ok && strings.HasPrefix(profileType, "goroutine:") {
				t.Errorf("obsolete singular goroutine profile type at %s: %s", path, profileType)
			}
			if selector, ok := value["labelSelector"].(string); ok {
				if strings.Contains(selector, "version=") {
					t.Errorf("Pyroscope selector relies on absent version label at %s: %s", path, selector)
				}
				// The server tags profiles with the same namespace,
				// cluster and node identity as the metrics; service_name
				// is a per-deployment application name and matches
				// nothing the dashboard variables select.
				for _, want := range []string{
					`k8s_namespace_name=~"$namespace"`,
					`formance_ledger_cluster_name=~"$cluster"`,
					`formance_ledger_node_id=~"$node"`,
				} {
					if !strings.Contains(selector, want) {
						t.Errorf("Pyroscope selector at %s is not scoped by %s: %s", path, want, selector)
					}
				}
				if strings.Contains(selector, "service_name") {
					t.Errorf("Pyroscope selector hard-codes a service name at %s: %s", path, selector)
				}
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
		raw = resourceAttributeLabels.Replace(raw)
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
		content := resourceAttributeLabels.Replace(string(raw))
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
