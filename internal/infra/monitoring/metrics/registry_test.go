package metrics_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/monitoring/metrics"
)

// TestMetricsRegistry verifies that the human-maintained registry in
// misc/devenv/monitoring-dashboards/jsonnet/lib/metrics.libsonnet is
// kept in sync with what the application actually emits.
//
// The dashboard regenerates from that registry, so a drift here is
// silent — panels would lose data and operators would have no
// indication anything is wrong until the dashboard is opened on a
// real cluster. The check therefore runs as a regular Go unit test.
//
// Both directions are enforced:
//   - every metric name in the registry must be created by at least
//     one call site in the codebase;
//   - every instrument our code creates (regardless of meter name)
//     must appear in the registry.
//
// OpenTelemetry semantic-convention auto-instrumentation (go.*,
// process.*, system.*, http.*) targets the *global* MeterProvider —
// our code does not emit those names, so they don't appear here.
func TestMetricsRegistry(t *testing.T) {
	t.Parallel()

	repoRoot := findRepoRoot(t)

	registryPath := filepath.Join(repoRoot, "misc", "devenv", "monitoring-dashboards", "jsonnet", "lib", "metrics.libsonnet")
	registry := parseRegistry(t, registryPath)

	codeNames := instrumentNames(t)

	for _, name := range registry {
		require.Contains(t, codeNames, name,
			"metric %q is listed in metrics.libsonnet but no call site emits it — either remove it from the registry or restore the call site",
			name)
	}

	registrySet := make(map[string]struct{}, len(registry))
	for _, n := range registry {
		registrySet[n] = struct{}{}
	}
	for _, n := range codeNames {
		if _, ok := registrySet[n]; !ok {
			t.Errorf("metric %q is emitted in the code but is missing from metrics.libsonnet — add it to the registry so the dashboard can reference it", n)
		}
	}
}

// TestMetricsDocumented keeps docs/ops/monitoring.md, the operator reference
// for every metric, in sync with the instruments the code creates (RFC-0021:
// a change that adds a formance. name documents it in the same change).
func TestMetricsDocumented(t *testing.T) {
	t.Parallel()

	doc, err := os.ReadFile(filepath.Join(findRepoRoot(t), "docs", "ops", "monitoring.md"))
	require.NoError(t, err)

	// A metric table row reads | `name` [/ `name`] | Type | Unit | Description |.
	type entry struct{ kind, unit string }
	rows := make(map[string]entry)
	quoted := regexp.MustCompile("`([a-z0-9_.]+)`")
	for line := range strings.SplitSeq(string(doc), "\n") {
		cells := strings.Split(line, "|")
		if len(cells) < 5 || !strings.HasPrefix(strings.TrimSpace(cells[1]), "`") {
			continue
		}
		// Other tables (naming examples, profile labels) also start with a
		// backticked name; only metric tables carry an instrument kind.
		switch strings.TrimSpace(cells[2]) {
		case "Counter", "Counter (observable)", "UpDownCounter", "Histogram", "Gauge":
		default:
			continue
		}
		unit := strings.Trim(strings.TrimSpace(cells[3]), "`")
		if unit == "-" {
			unit = ""
		}
		for _, m := range quoted.FindAllStringSubmatch(cells[1], -1) {
			rows[m[1]] = entry{kind: strings.TrimSpace(cells[2]), unit: unit}
		}
	}

	for _, inst := range collectInstruments(t) {
		row, ok := rows[inst.name]
		if !ok {
			t.Errorf("metric %q has no row in a docs/ops/monitoring.md metric table", inst.name)

			continue
		}
		if want := docKind(inst.method); row.kind != want {
			t.Errorf("metric %q is documented as %q, but %s creates it as %s", inst.name, row.kind, inst.method, want)
		}
		if row.unit != inst.unit {
			t.Errorf("metric %q is documented with unit %q, but the code declares %q", inst.name, row.unit, inst.unit)
		}
	}
}

// docKind is the Type column docs/ops/monitoring.md uses for a constructor.
func docKind(method string) string {
	base := strings.TrimPrefix(strings.TrimPrefix(method, "Int64"), "Float64")
	switch base {
	case "ObservableCounter":
		return "Counter (observable)"
	case "ObservableGauge":
		return "Gauge"
	case "ObservableUpDownCounter":
		return "UpDownCounter"
	default:
		return base
	}
}

// TestMetricMetadataMatchesCode keeps the dashboard generator's metadata,
// which drives the unit and _total suffixes of the normalised Prometheus
// names, in line with every instrument the code creates.
func TestMetricMetadataMatchesCode(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join(findRepoRoot(t), "misc", "devenv", "monitoring-dashboards", "jsonnet", "lib", "metric_metadata.libsonnet"))
	require.NoError(t, err)
	type entry struct{ kind, unit string }
	metadata := make(map[string]entry)
	line := regexp.MustCompile(`'([a-z0-9_.]+)':\s*\{\s*kind:\s*'([a-z]+)',\s*unit:\s*(null|'[^']*')\s*\}`)
	for _, m := range line.FindAllStringSubmatch(string(data), -1) {
		unit := ""
		if m[3] != "null" {
			unit = strings.Trim(m[3], "'")
		}
		metadata[m[1]] = entry{kind: m[2], unit: unit}
	}

	for _, inst := range collectInstruments(t) {
		meta, ok := metadata[inst.name]
		if !ok {
			t.Errorf("metric %q has no entry in metric_metadata.libsonnet", inst.name)

			continue
		}
		if want := metadataKind(inst.method); meta.kind != want {
			t.Errorf("metric %q has metadata kind %q, but %s is a %s", inst.name, meta.kind, inst.method, want)
		}
		if meta.unit != inst.unit {
			t.Errorf("metric %q has metadata unit %q, but the code declares %q", inst.name, meta.unit, inst.unit)
		}
	}
}

// metadataKind is the kind metric_metadata.libsonnet uses for a constructor.
func metadataKind(method string) string {
	base := strings.TrimPrefix(strings.TrimPrefix(method, "Int64"), "Float64")
	base = strings.TrimPrefix(base, "Observable")

	return strings.ToLower(base)
}

// TestInstrumentNamesCarryNoUnit enforces the OpenTelemetry naming
// guideline (and RFC-0021 rule 3) that a metric name never spells its
// unit: the unit belongs in metric.WithUnit, and the Prometheus
// translation appends it, so `pebble.flush.duration.milliseconds` would
// surface as `..._milliseconds_milliseconds` or lose the unit entirely.
func TestInstrumentNamesCarryNoUnit(t *testing.T) {
	t.Parallel()

	unitWords := map[string]struct{}{
		"ns": {}, "us": {}, "ms": {}, "nanoseconds": {}, "microseconds": {},
		"milliseconds": {}, "seconds": {}, "bytes": {}, "percent": {},
	}
	for _, name := range instrumentNames(t) {
		words := strings.FieldsFunc(name, func(r rune) bool { return r == '.' || r == '_' })
		for _, word := range words {
			if _, ok := unitWords[word]; ok {
				t.Errorf("metric %q spells its unit %q in the name; drop it and set metric.WithUnit instead", name, word)
			}
		}
		// Counters must not end in total either: the Prometheus translation
		// appends _total, and delta backends would read it literally.
		if words[len(words)-1] == "total" {
			t.Errorf("metric %q ends in total; name the counted thing in the plural instead (e.g. pebble.flushes)", name)
		}
	}
}

// TestInstrumentUnits enforces the OpenTelemetry unit guidelines
// (https://opentelemetry.io/docs/specs/semconv/general/metrics/#instrument-units):
//   - durations are measured in seconds (`s`), on a Float64 instrument so
//     sub-second values are not truncated;
//   - `1` denotes a dimensionless value (a ratio or a utilization), which
//     only a gauge can hold; integer counts of things use a UCUM annotation
//     such as `{entry}` instead.
func TestInstrumentUnits(t *testing.T) {
	t.Parallel()

	annotation := regexp.MustCompile(`^\{[a-z_]+\}$`)
	for _, inst := range collectInstruments(t) {
		path, method, name := inst.path, inst.method, inst.name
		switch u := inst.unit; u {
		case "":
		case "1":
			if !strings.HasSuffix(method, "Gauge") {
				t.Errorf("%s: %s %q uses unit \"1\", which denotes a ratio; use a UCUM annotation such as \"{entry}\"", path, method, name)
			}
		case "ns", "us", "ms", "min", "h", "nanoseconds", "microseconds", "milliseconds", "seconds":
			// OpenTelemetry: durations SHOULD be measured in seconds.
			t.Errorf("%s: %s %q measures a duration in %q; use \"s\" with a Float64 instrument and record d.Seconds()", path, method, name, u)
		case "bytes":
			t.Errorf("%s: %s %q spells its unit; use the UCUM code \"By\"", path, method, name)
		case "s":
			if strings.HasPrefix(method, "Int64") {
				t.Errorf("%s: %s %q measures seconds with an integer instrument, which truncates sub-second durations; use the Float64 variant", path, method, name)
			}
		case "By":
		default:
			// Counts use a singular annotation: {entry}, not {entries}
			// ({miss} and {status} end in s but are singular).
			plural := strings.HasSuffix(u, "s}") && !strings.HasSuffix(u, "ss}") && !strings.HasSuffix(u, "us}")
			if !annotation.MatchString(u) || plural {
				t.Errorf("%s: %s %q has unit %q; use s, By, 1 (gauge ratios) or a singular annotation such as {entry}", path, method, name, u)
			}
		}
	}
}

// TestInstrumentNamesFollowKind enforces the OpenTelemetry naming guidelines
// that depend on the instrument kind
// (https://opentelemetry.io/docs/specs/semconv/general/naming/):
//   - a counter of discrete things is named in the plural (pebble.flushes,
//     raft.fsm.logs_appended): some word of its last segment is plural;
//   - an UpDownCounter is not pluralized; it uses .count instead;
//   - a duration, i.e. an instrument measured in seconds, ends in .duration.
func TestInstrumentNamesFollowKind(t *testing.T) {
	t.Parallel()

	syntax := regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$`)
	instruments := collectInstruments(t)
	names := make(map[string]struct{}, len(instruments))
	for _, inst := range instruments {
		names[inst.name] = struct{}{}
	}
	for _, inst := range instruments {
		path, method, name := inst.path, inst.method, inst.name
		if !syntax.MatchString(name) {
			t.Errorf("%s: %q is not lowercase dot-separated segments joined by _ within a segment", path, name)

			continue
		}
		segments := strings.Split(name, ".")
		last := segments[len(segments)-1]
		switch {
		case strings.HasSuffix(method, "UpDownCounter"):
			if strings.HasSuffix(last, "s") {
				t.Errorf("%s: UpDownCounter %q is pluralized; name it <thing>.count", path, name)
			}
		case strings.HasSuffix(method, "Counter"):
			plural := false
			for word := range strings.SplitSeq(last, "_") {
				plural = plural || strings.HasSuffix(word, "s")
			}
			if !plural {
				t.Errorf("%s: counter %q is not named in the plural (e.g. %s.%ss)", path, name, strings.Join(segments[:len(segments)-1], "."), last)
			}
		}
		if inst.unit == "s" && last != "duration" {
			t.Errorf("%s: %q measures seconds but does not end in .duration", path, name)
		}
		// A metric name must not double as the namespace of another metric:
		// raft.foo next to raft.foo.bar reads as one entity in backends that
		// nest names.
		for i := 1; i < len(segments); i++ {
			if _, ok := names[strings.Join(segments[:i], ".")]; ok {
				t.Errorf("%s: %q nests under %q, which is itself a metric", path, name, strings.Join(segments[:i], "."))
			}
		}
	}
}

// TestRecordedDurationsAreSeconds complements TestInstrumentUnits: every
// duration instrument declares seconds, so no Record call may pass a
// time.Duration converted to another unit, even wrapped in float64(...)
// where the compiler would accept it.
func TestRecordedDurationsAreSeconds(t *testing.T) {
	t.Parallel()

	record := regexp.MustCompile(`\.Record\(`)
	subSecond := regexp.MustCompile(`\.(Nanoseconds|Microseconds|Milliseconds)\(\)`)
	walkSources(t, func(path string, data []byte) {
		for _, r := range record.FindAllIndex(data, -1) {
			open := r[1] - 1
			if m := subSecond.Find(data[open:callEnd(data, open)]); m != nil {
				line := 1 + strings.Count(string(data[:r[0]]), "\n")
				t.Errorf("%s:%d: Record passes a duration as %s; duration instruments use seconds, record d.Seconds()", path, line, m)
			}
		}
	})
}

// walkSources calls fn with the content of every non-test Go file under
// internal/ and cmd/, the two trees that build the ledger binaries.
func walkSources(t *testing.T, fn func(path string, data []byte)) {
	t.Helper()

	repoRoot := findRepoRoot(t)
	for _, root := range []string{"internal", "cmd"} {
		err := filepath.Walk(filepath.Join(repoRoot, root), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			fn(path, data)

			return nil
		})
		require.NoError(t, err)
	}
}

// callEnd returns the index just past the parenthesis that closes the one at
// data[open], skipping parentheses inside string and rune literals. It
// returns len(data) when the call is unbalanced.
func callEnd(data []byte, open int) int {
	depth := 0
	for i := open; i < len(data); i++ {
		switch data[i] {
		case '"', '\'', '`':
			quote := data[i]
			for i++; i < len(data) && data[i] != quote; i++ {
				if data[i] == '\\' && quote != '`' {
					i++
				}
			}
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}

	return len(data)
}

// TestNamingPolicyMatchesDashboards verifies the invariants the
// dashboard generator in misc/devenv/monitoring-dashboards relies on
// to mirror the go-libs metrics prefix (metrics.PrefixedName):
//   - the Jsonnet prefix equals [metrics.DefaultPrefix];
//   - no instrument our code creates starts with a semantic-convention
//     prefix listed in `semconvPrefixes`, which the generator never
//     prefixes while the go-libs prefixed provider always does;
//   - no instrument already carries the default prefix, which would
//     be emitted twice.
func TestNamingPolicyMatchesDashboards(t *testing.T) {
	t.Parallel()

	repoRoot := findRepoRoot(t)
	data, err := os.ReadFile(filepath.Join(repoRoot, "misc", "devenv", "monitoring-dashboards", "jsonnet", "lib", "naming.libsonnet"))
	require.NoError(t, err)

	prefixMatch := regexp.MustCompile(`(?m)^\s*prefix:: '([^']*)',`).FindSubmatch(data)
	require.NotNil(t, prefixMatch, "naming.libsonnet must declare `prefix:: '...'`")
	require.Equal(t, metrics.DefaultPrefix, string(prefixMatch[1]),
		"naming.libsonnet prefix must match metrics.DefaultPrefix")

	semconvBlock := regexp.MustCompile(`(?s)semconvPrefixes:: \[(.*?)\]`).FindSubmatch(data)
	require.NotNil(t, semconvBlock, "naming.libsonnet must declare `semconvPrefixes:: [...]`")
	semconvPrefixes := regexp.MustCompile(`'([^']+)'`).FindAllSubmatch(semconvBlock[1], -1)
	require.NotEmpty(t, semconvPrefixes)

	for _, name := range instrumentNames(t) {
		for _, sc := range semconvPrefixes {
			require.False(t, strings.HasPrefix(name, string(sc[1])),
				"instrument %q starts with semantic-convention prefix %q: the dashboards would not prefix it while the server does", name, sc[1])
		}
		require.False(t, strings.HasPrefix(name, metrics.DefaultPrefix+"."),
			"instrument %q already carries the %q prefix and would be emitted with it twice", name, metrics.DefaultPrefix)
	}
}

// TestSemconvInstrumentationKeepsGlobalProvider guards the mechanism that
// keeps OpenTelemetry semantic-convention metrics (go.*, process.*,
// system.*, http.*, rpc.*) out of the metrics prefix: the Go runtime,
// host, otelhttp and otelgrpc instrumentation record through the global
// MeterProvider, which go-libs sets to the raw SDK provider, while only
// the injected provider is prefixed. Installing a global provider or
// handing a provider to instrumentation libraries in production code
// could route semconv metrics through the prefixed provider.
func TestSemconvInstrumentationKeepsGlobalProvider(t *testing.T) {
	t.Parallel()

	repoRoot := findRepoRoot(t)
	forbidden := regexp.MustCompile(`\botel\.SetMeterProvider\(|\bWithMeterProvider\(`)

	for _, dir := range []string{"internal", "cmd", "pkg"} {
		root := filepath.Join(repoRoot, dir)
		if _, err := os.Stat(root); os.IsNotExist(err) {
			continue
		}
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if loc := forbidden.FindIndex(data); loc != nil {
				rel, _ := filepath.Rel(repoRoot, path) // best effort: only used in the message
				t.Errorf("%s: %q routes a MeterProvider into OpenTelemetry instrumentation; semantic-convention metrics must keep using the raw global provider so the metrics prefix never renames them",
					rel, data[loc[0]:loc[1]])
			}

			return nil
		})
		require.NoError(t, err)
	}
}

// findRepoRoot walks up from the working directory until it finds
// the repository's go.mod file. Fails the test if no go.mod is
// found.
func findRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	require.NoError(t, err)
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not locate repository root from %s", wd)
		}
		dir = parent
	}
}

// parseRegistry extracts every metric name from the libsonnet
// registry. Names appear as single-quoted string values on the
// right-hand side of a field assignment, e.g.
// `ready: 'bloom.ready',`. Names without a dot are not real OTel
// metric names (an unrelated helper field could legitimately be a
// single word), so the extraction filters to dotted strings only.
func parseRegistry(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	re := regexp.MustCompile(`'([a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+)'`)
	matches := re.FindAllStringSubmatch(string(data), -1)
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, m[1])
	}
	sort.Strings(out)

	return out
}

// instrumentMethods are the synchronous and observable instrument
// constructors exposed by go.opentelemetry.io/otel/metric.Meter.
var instrumentMethods = []string{
	"Int64Counter",
	"Int64UpDownCounter",
	"Int64Histogram",
	"Int64Gauge",
	"Int64ObservableCounter",
	"Int64ObservableUpDownCounter",
	"Int64ObservableGauge",
	"Float64Counter",
	"Float64UpDownCounter",
	"Float64Histogram",
	"Float64Gauge",
	"Float64ObservableCounter",
	"Float64ObservableUpDownCounter",
	"Float64ObservableGauge",
}

// instrument is one metric instrument the ledger creates, resolved from its
// constructor call site.
type instrument struct {
	path, method, name, unit string
}

// collectInstruments scans the non-test Go sources under internal/ and cmd/
// for instrument constructor calls and returns every instrument they create,
// with its name and declared unit. Anything our code instantiates is in scope:
// we don't filter by meter name because the metrics prefix applies uniformly
// to every meter obtained from the injected provider.
//
// A constructor whose name is not a string literal cannot be checked, so it
// fails the test, except in tailworker.RegisterTailGauges, whose instruments
// are expanded from each call site instead.
func collectInstruments(t *testing.T) []instrument {
	t.Helper()

	call := regexp.MustCompile(`\.(` + strings.Join(instrumentMethods, "|") + `)\(`)
	literal := regexp.MustCompile(`^\(\s*"([^"]+)"`)
	unit := regexp.MustCompile(`WithUnit\("([^"]*)"\)`)
	// tailworker.RegisterTailGauges(meter, ns, source, ...) creates three
	// gauges named from the ns/source literals, so its constructor calls are
	// not literals. Expand each call site into the triplet it emits, with the
	// units internal/pkg/tailworker/gauges.go declares.
	tailGauges := regexp.MustCompile(`RegisterTailGauges\(\s*[^,]+,\s*"([^"]+)"\s*,\s*"([^"]+)"`)
	tailworkerGauges := filepath.Join("internal", "pkg", "tailworker", "gauges.go")

	var out []instrument
	walkSources(t, func(path string, data []byte) {
		// Skip generated mocks: they re-declare the upstream interface
		// methods but never call them.
		if strings.Contains(path, "_generated") || strings.Contains(path, "/mock_") {
			return
		}
		for _, c := range call.FindAllSubmatchIndex(data, -1) {
			open := c[1] - 1
			args := data[open:callEnd(data, open)]
			m := literal.FindSubmatch(args)
			if m == nil {
				if !strings.HasSuffix(path, tailworkerGauges) {
					line := 1 + strings.Count(string(data[:c[0]]), "\n")
					t.Errorf("%s:%d: instrument name is not a string literal, so the metric guards cannot check it", path, line)
				}

				continue
			}
			declared := ""
			if u := unit.FindSubmatch(args); u != nil {
				declared = string(u[1])
			}
			out = append(out, instrument{path: path, method: string(data[c[2]:c[3]]), name: string(m[1]), unit: declared})
		}
		for _, m := range tailGauges.FindAllSubmatch(data, -1) {
			ns, source := string(m[1]), string(m[2])
			out = append(out,
				instrument{path: path, method: "Int64ObservableGauge", name: ns + ".last_indexed_sequence"},
				instrument{path: path, method: "Int64ObservableGauge", name: ns + "." + source + "_last_sequence"},
				instrument{path: path, method: "Int64ObservableGauge", name: ns + ".lag", unit: "{sequence}"},
			)
		}
	})

	return out
}

// instrumentNames returns the unique, sorted names of collectInstruments.
func instrumentNames(t *testing.T) []string {
	t.Helper()

	seen := make(map[string]struct{})
	for _, inst := range collectInstruments(t) {
		seen[inst.name] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)

	return out
}
