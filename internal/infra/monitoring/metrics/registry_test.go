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

	codeNames := collectInstrumentNamesFromCode(t, filepath.Join(repoRoot, "internal"))

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
	for _, name := range collectInstrumentNamesFromCode(t, filepath.Join(findRepoRoot(t), "internal")) {
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

// TestCountInstrumentsUseAnnotationUnits enforces the OpenTelemetry unit
// guideline (https://opentelemetry.io/docs/specs/semconv/general/metrics/#instrument-units)
// that `1` denotes a dimensionless value (a ratio or a utilization), which
// only a gauge can hold, while integer counts of things use a UCUM
// annotation such as `{entry}`.
func TestCountInstrumentsUseAnnotationUnits(t *testing.T) {
	t.Parallel()

	call := regexp.MustCompile(`\.(` + strings.Join(instrumentMethods, "|") + `)\(\s*"([^"]+)"`)
	unit := regexp.MustCompile(`WithUnit\("([^"]*)"\)`)
	err := filepath.Walk(filepath.Join(findRepoRoot(t), "internal"), func(path string, info os.FileInfo, err error) error {
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
		calls := call.FindAllSubmatchIndex(data, -1)
		for i, c := range calls {
			// An instrument's options end before the next constructor call.
			end := len(data)
			if i+1 < len(calls) {
				end = calls[i+1][0]
			}
			method, name := string(data[c[2]:c[3]]), string(data[c[4]:c[5]])
			m := unit.FindSubmatch(data[c[1]:end])
			if m != nil && string(m[1]) == "1" && !strings.HasSuffix(method, "Gauge") {
				t.Errorf("%s: %s %q uses unit \"1\", which denotes a ratio; use a UCUM annotation such as \"{entry}\"", path, method, name)
			}
		}

		return nil
	})
	require.NoError(t, err)
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

	for _, name := range collectInstrumentNamesFromCode(t, filepath.Join(repoRoot, "internal")) {
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

// collectInstrumentNamesFromCode scans the .go files under root for
// call sites that create instruments and returns the set of unique
// instrument names. Anything our code instantiates is in scope —
// we don't filter by meter name because the metrics prefix applies
// uniformly to every meter obtained from the injected provider.
func collectInstrumentNamesFromCode(t *testing.T, root string) []string {
	t.Helper()
	pattern := regexp.MustCompile(
		`\.` + "(" + strings.Join(instrumentMethods, "|") + ")" + `\(\s*"([^"]+)"`,
	)
	// tailworker.RegisterTailGauges(meter, ns, source, ...) creates its three
	// instruments with names built from the ns/source literals rather than a
	// single literal at the Int64ObservableGauge call site, so the pattern
	// above cannot see them. Expand each call site into the triplet it emits:
	// {ns}.last_indexed_sequence, {ns}.{source}_last_sequence, {ns}.lag.
	tailGaugePattern := regexp.MustCompile(`RegisterTailGauges\([^,]+,\s*"([^"]+)"\s*,\s*"([^"]+)"`)
	seen := make(map[string]struct{})

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		// Skip generated mocks: they re-declare the upstream interface
		// methods but never call them.
		if strings.Contains(path, "_generated") || strings.Contains(path, "/mock_") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		matches := pattern.FindAllSubmatch(data, -1)
		for _, m := range matches {
			name := string(m[2])
			if name != "" {
				seen[name] = struct{}{}
			}
		}

		for _, m := range tailGaugePattern.FindAllSubmatch(data, -1) {
			ns := string(m[1])
			source := string(m[2])
			seen[ns+".last_indexed_sequence"] = struct{}{}
			seen[ns+"."+source+"_last_sequence"] = struct{}{}
			seen[ns+".lag"] = struct{}{}
		}

		return nil
	})
	require.NoError(t, err)

	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)

	return out
}
